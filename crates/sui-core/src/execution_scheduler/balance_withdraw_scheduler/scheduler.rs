// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This modules implements an optimistic scheduler for balance withdraw reservations.
//! A transaction that contains withdraw reservations when enqueued through execution scheduler,
//! will first be enqueued here and wait for notifications.
//! Only after the withdraw reservations are secured it will then proceed to
//! check input object dependencies and eventually proceed to execution.
//! Note that when checking input object dependencies, a transaction no longer
//! needs to check the dependencies on the accumulator root object, because that dependency
//! is managed by this module.
//!
//! When a transaction is enqueued here, a conservative way to process it would be to
//! wait until the dependent accumulator version is fully settled when the settlement
//! transaction is executed. At that point we must have reached a deterministic state
//! where we know for sure if there is enough balance for the withdraw reservations.
//! However, that would significantly limit the throughput of the system.
//!
//! Instead, we use an optimistic approach where we track the guaranteed minimum balance
//! for each account, by using a recent settled balance in this account, together with all
//! pending withdraws that have been reserved for this account.
//!
//! When a transaction is enqueued here, we collect all the withdraw reservations
//! and their associated accounts.
//! We immediately try to see that for each account, whether we can guarantee it
//! will have enough balance to satisfy the withdraw reservation.
//! Similarly, whenever a settlement transaction is executed, we update the guaranteed minimum balance
//! for each account, and try to reserve the withdraws for each account again.
//!
//! In either case if we know for sure that all withdraw reservations in a transaction
//! can be satisfied, we send a notification to the transaction so it can
//! proceed to execution without waiting for the accumulator version to be fully settled.
//!
//! The implementation contains two critical entry points:
//! 1. When a transaction is enqueued, we collect all the withdraw reservations and their associated accounts.
//!    We then try to reserve the withdraws for each account.
//! 2. When a settlement transaction is executed, we update the guaranteed minimum balance for each account,
//!    and try to reserve the withdraws for each account again.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use dashmap::DashMap;
use mysten_metrics::monitored_mpsc::{self, UnboundedReceiver, UnboundedSender};
use parking_lot::RwLock;
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    digests::TransactionDigest,
};
use tokio::sync::watch;

use crate::execution_scheduler::balance_withdraw_scheduler::{
    account_state::{AccountState, TxWithdrawRegistration},
    balance_read::AccountBalanceRead,
    BalanceSettlement, ScheduleResult, TxBalanceWithdraw,
};

/// All transactions that have scheduled withdraw reservations at a specific accumulator version,
/// and the set of accounts that have scheduled withdraw reservations at that accumulator version.
/// This is used to make sure we don't schedule the same transaction twice,
/// as welll as used to settle the withdraw reservations for all accounts
/// that reserved withdraws at each accumulator version.
#[allow(dead_code)]
#[derive(Default)]
struct PendingAccumulatorVersion {
    transactions: BTreeSet<TransactionDigest>,
    accounts: BTreeSet<ObjectID>,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct WithdrawScheduler {
    /// All accounts that we need to track the status.
    /// These are accounts that have scheduled withdraw reservations,
    /// and have not yet been settled.
    account_states: Arc<DashMap<ObjectID, AccountState>>,
    /// The last settled accumulator version.
    last_known_settled_version: Arc<RwLock<SequenceNumber>>,
    /// For each accumulator version, the set of transactions and the set of account IDs
    /// that have scheduled withdraw reservations at that accumulator version,
    /// but have not yet been settled.
    pending_accumulator_versions: Arc<DashMap<SequenceNumber, PendingAccumulatorVersion>>,
    /// Channel that allows us to receive and process balance settlements asynchronously.
    settlement_sender: UnboundedSender<BalanceSettlement>,
}

impl WithdrawScheduler {
    pub fn new(balance_read: &dyn AccountBalanceRead) -> Arc<Self> {
        let (settlement_sender, settlement_receiver) =
            monitored_mpsc::unbounded_channel("balance_withdraw_scheduler_settlement");
        let cur_accumulator_version = balance_read.get_accumulator_version();
        let scheduler = Arc::new(Self {
            account_states: Arc::new(DashMap::new()),
            last_known_settled_version: Arc::new(RwLock::new(cur_accumulator_version)),
            pending_accumulator_versions: Arc::new(DashMap::new()),
            settlement_sender,
        });
        let scheduler_clone = scheduler.clone();
        tokio::spawn(async move {
            scheduler_clone
                .process_settlement_task(settlement_receiver)
                .await;
        });
        scheduler
    }

    async fn process_settlement_task(
        self: Arc<Self>,
        mut settlement_receiver: UnboundedReceiver<BalanceSettlement>,
    ) {
        let mut pending_settlements = BTreeMap::new();
        let mut expected_version = *self.last_known_settled_version.read();
        while let Some(settlement) = settlement_receiver.recv().await {
            pending_settlements.insert(settlement.old_accumulator_version, settlement);
            while let Some(settlement) = pending_settlements.remove(&expected_version) {
                expected_version = settlement.new_accumulator_version;
                self.process_settlement(settlement);
            }
        }
    }

    fn process_settlement(&self, settlement: BalanceSettlement) {
        let mut last_known_settled_version = self.last_known_settled_version.write();
        *last_known_settled_version = settlement.new_accumulator_version;

        let mut unique_accounts = settlement
            .balance_changes
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        if let Some((_, pending)) = self
            .pending_accumulator_versions
            .remove(&settlement.old_accumulator_version)
        {
            unique_accounts.extend(pending.accounts.clone());
            for account_id in pending.accounts {
                let mut account_state = self.account_states.get_mut(&account_id).unwrap();
                account_state.settle_accumulator_version(settlement.old_accumulator_version);
            }
        }

        for (account_id, balance_change) in settlement.balance_changes {
            let Some(mut account_state) = self.account_states.get_mut(&account_id) else {
                continue;
            };
            account_state.settle_balance_change(balance_change);
            // TODO: Check consistency in debug mode.
        }

        for account_id in unique_accounts {
            self.try_reserve_withdraws_for_account(account_id, settlement.new_accumulator_version);
        }
    }

    pub fn handle_balance_settlement(&self, settlement: BalanceSettlement) {
        if let Err(err) = self.settlement_sender.send(settlement) {
            tracing::error!("Failed to send balance settlement: {:?}", err);
        }
    }

    pub async fn enqueue_withdraw_reservations(
        &self,
        accumulator_version: SequenceNumber,
        withdraws: Vec<TxBalanceWithdraw>,
        balance_read: &dyn AccountBalanceRead,
    ) -> BTreeMap<TransactionDigest, watch::Receiver<ScheduleResult>> {
        if withdraws.is_empty() {
            return BTreeMap::new();
        }
        let guard = self.last_known_settled_version.read();
        let last_known_settled_version = *guard;
        if last_known_settled_version > accumulator_version {
            // Accumulator object version can only be updated through settlement transactions.
            // A settlement transaction can only be executed after all previous transactions have been
            // executed, because the construction of a settlement transaction requires the effects of
            // all previous transactions from the same consensus commit.
            // Because of this, if the current accumulator version is greater than the
            // accumulator version of the transactions to be scheduled, we know that the transactions
            // must have already been executed. And hence there is no need to schedule the withdraw
            // reservations for these transactions.
            let receivers = withdraws
                .into_iter()
                .map(|withdraw| {
                    let (_, notify_receiver) = watch::channel(ScheduleResult::AlreadyExecuted);
                    (withdraw.tx_digest, notify_receiver)
                })
                .collect();
            return receivers;
        }
        let withdraws_to_schedule: Vec<_> = {
            let mut entry = self
                .pending_accumulator_versions
                .entry(accumulator_version)
                .or_default();
            withdraws
                .into_iter()
                .filter(|withdraw| {
                    if entry.transactions.insert(withdraw.tx_digest) {
                        entry.accounts.extend(withdraw.reservations.keys().copied());
                        true
                    } else {
                        false
                    }
                })
                .collect()
        };

        let mut receivers = BTreeMap::new();
        let mut all_registrations: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for withdraw in withdraws_to_schedule {
            let (registration, notify_receiver) =
                TxWithdrawRegistration::new(accumulator_version, withdraw.reservations);
            receivers.insert(withdraw.tx_digest, notify_receiver);
            for (account_id, _) in registration.pending_accounts.read().iter() {
                all_registrations
                    .entry(*account_id)
                    .or_default()
                    .push(registration.clone());
            }
        }

        for (account_id, registrations) in all_registrations {
            self.account_states
                .entry(account_id)
                .or_insert_with(|| {
                    AccountState::new(balance_read, &account_id, last_known_settled_version)
                })
                .add_registrations(registrations);
            self.try_reserve_withdraws_for_account(account_id, last_known_settled_version);
        }
        receivers
    }

    fn try_reserve_withdraws_for_account(
        &self,
        account_id: ObjectID,
        last_known_settled_version: SequenceNumber,
    ) -> Vec<Arc<TxWithdrawRegistration>> {
        let mut scheduled_registrations = Vec::new();
        {
            let Some(mut account_state) = self.account_states.get_mut(&account_id) else {
                return scheduled_registrations;
            };
            while let Some(registration) =
                account_state.try_schedule_next_reservation(&account_id, last_known_settled_version)
            {
                scheduled_registrations.push(registration);
            }
        }
        self.account_states
            .remove_if(&account_id, |_, account_state| account_state.is_empty());
        scheduled_registrations
    }
}
