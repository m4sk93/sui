// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use parking_lot::{Mutex, RwLock};
use tokio::sync::watch;

use sui_types::base_types::{ObjectID, SequenceNumber};

use crate::execution_scheduler::balance_withdraw_scheduler::{
    balance_read::AccountBalanceRead, ScheduleResult,
};

/// Represents the state of an account that has scheduled withdraw reservations.
#[allow(dead_code)]
pub(crate) struct AccountState {
    /// Transactions that contain withdraw reservations in this account.
    /// We have not yet reserved these withdraws because we cannot
    /// yet guarantee that there is enough balance for them, and we have
    /// not yet settled the dependent accumulator version.
    pub(crate) enqueued: VecDeque<Arc<TxWithdrawRegistration>>,
    /// Map from each accumulator version to the total amount of withdraws that
    /// have been currently reserved at that accumulator version.
    /// Each entry will be removed when the accumulator version is settled,
    /// and the amount will be added back to the last_known_min_balance.
    pub(crate) reserved: BTreeMap<SequenceNumber, u64>,
    /// The guaranteed minimum balance of the account based on the most recent settlement,
    /// as well as all reserved withdraws.
    pub(crate) last_known_min_balance: i128,
}

/// Represents an active registration of a transaction waiting for its withdraw
/// reservations to be deterministically satisfied.
#[allow(dead_code)]
pub(crate) struct TxWithdrawRegistration {
    /// The accumulator version at which the transaction was scheduled on.
    pub accumulator_version: SequenceNumber,
    /// The set of accounts and the amount of withdraw reservations in this transaction
    /// that are not yet deterministic where we know for sure
    /// if there is enough balance for the reservation on this account.
    /// When this becomes empty, the transaction is ready to be executed
    /// at least from balance withdraw perspective.
    pub pending_accounts: RwLock<BTreeMap<ObjectID, u64>>,
    /// The channel to notify the transaction when it is ready to be executed.
    pub notify: Mutex<Option<watch::Sender<ScheduleResult>>>,
}

impl AccountState {
    pub(crate) fn new(
        balance_read: &dyn AccountBalanceRead,
        account_id: &ObjectID,
        last_known_settled_version: SequenceNumber,
    ) -> Self {
        let cur_balance = balance_read.get_account_balance(account_id, last_known_settled_version);
        Self {
            enqueued: VecDeque::new(),
            reserved: BTreeMap::new(),
            last_known_min_balance: cur_balance as i128,
        }
    }

    pub(crate) fn add_registrations(&mut self, registrations: Vec<Arc<TxWithdrawRegistration>>) {
        self.enqueued.extend(registrations);
    }

    /// Try to schedule the next reservation for the given account.
    /// The next reservation can be scheduled if either:
    /// 1. The accumulator version of the transaction is the same as the last known settled version,
    ///    meaning that the previous accumulator version has been fully settled.
    /// 2. We can guarantee that the account has enough balance to satisfy the reservation.
    pub(crate) fn try_schedule_next_reservation(
        &mut self,
        account_id: &ObjectID,
        last_known_settled_version: SequenceNumber,
    ) -> Option<Arc<TxWithdrawRegistration>> {
        let registration = self.enqueued.front()?;
        let reserved_amount = *registration
            .pending_accounts
            .read()
            .get(account_id)
            .unwrap();
        if registration.accumulator_version > last_known_settled_version
            && reserved_amount as i128 > self.last_known_min_balance
        {
            return None;
        }

        let registration = self.enqueued.pop_front().unwrap();
        let executing = self
            .reserved
            .entry(registration.accumulator_version)
            .or_default();
        *executing += reserved_amount;
        self.last_known_min_balance -= reserved_amount as i128;
        registration.account_ready(account_id);
        // TODO: Here we could check balance consistency in debug mode.

        Some(registration)
    }

    pub(crate) fn settle_accumulator_version(&mut self, accumulator_version: SequenceNumber) {
        let executing_reservations = self.reserved.remove(&accumulator_version).unwrap();
        self.last_known_min_balance += executing_reservations as i128;
    }

    pub(crate) fn settle_balance_change(&mut self, balance_change: i128) {
        self.last_known_min_balance += balance_change;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.enqueued.is_empty() && self.reserved.is_empty()
    }
}

impl TxWithdrawRegistration {
    pub(crate) fn new(
        accumulator_version: SequenceNumber,
        pending_accounts: BTreeMap<ObjectID, u64>,
    ) -> (Arc<Self>, watch::Receiver<ScheduleResult>) {
        let (notify_sender, mut notify_receiver) = watch::channel(ScheduleResult::Init);
        // Consume the initial value of the receiver, as it is not meaningful.
        notify_receiver.borrow_and_update();
        let registration = Arc::new(Self {
            accumulator_version,
            pending_accounts: RwLock::new(pending_accounts),
            notify: Mutex::new(Some(notify_sender)),
        });
        (registration, notify_receiver)
    }

    /// Signals a specific account has reached a deterministic state where
    /// we know for sure if there is enough balance for the reservation on this account.
    /// If all accounts are ready, the transaction is ready to be executed.
    fn account_ready(&self, account_id: &ObjectID) {
        // Because we register each transaction exactly once on each account,
        // and we pop that registration when we process it, it is guaranteed
        // that the account is in the pending accounts map and gets removed
        // exactly once.
        assert!(self.pending_accounts.write().remove(account_id).is_some());
        if self.pending_accounts.read().is_empty() {
            if let Some(notify) = self.notify.lock().take() {
                let _ = notify.send(ScheduleResult::ReadyForExecution);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_scheduler::balance_withdraw_scheduler::balance_read::MockBalanceRead;

    #[tokio::test]
    async fn test_account_state_new() {
        let account_id = ObjectID::random();
        let balance = 1000;
        let version = SequenceNumber::from_u64(1);

        let mut mock_balance_read = MockBalanceRead::new(version);
        mock_balance_read.settle_balance_changes(version, BTreeMap::from([(account_id, balance)]));

        let account_state = AccountState::new(&mock_balance_read, &account_id, version);
        assert_eq!(account_state.last_known_min_balance, balance);
        assert!(account_state.enqueued.is_empty());
        assert!(account_state.reserved.is_empty());
    }

    #[test]
    fn test_non_existent_account() {
        let account_id = ObjectID::random();
        let version = SequenceNumber::from_u64(1);
        let mock_balance_read = MockBalanceRead::new(version);
        let account_state = AccountState::new(&mock_balance_read, &account_id, version);
        assert_eq!(account_state.last_known_min_balance, 0);
    }

    #[tokio::test]
    async fn test_account_state_add_registrations() {
        let account_id = ObjectID::random();
        let version = SequenceNumber::from_u64(1);
        let mock_balance_read = MockBalanceRead::new(version);

        let mut account_state = AccountState::new(&mock_balance_read, &account_id, version);

        let mut accounts = BTreeMap::new();
        accounts.insert(account_id, 100);
        let (registration, _) = TxWithdrawRegistration::new(version, accounts);

        account_state.add_registrations(vec![registration]);
        assert_eq!(account_state.enqueued.len(), 1);
    }

    #[tokio::test]
    async fn test_account_state_try_schedule_next_reservation() {
        let account_id = ObjectID::random();
        let version = SequenceNumber::from_u64(1);
        let mut mock_balance_read = MockBalanceRead::new(version);
        mock_balance_read.set_balance(account_id, 1000);

        let mut account_state = AccountState::new(&mock_balance_read, &account_id, version);

        // Add a registration with withdraw amount less than balance
        let mut accounts = BTreeMap::new();
        accounts.insert(account_id, 500);
        let (registration, receiver) = TxWithdrawRegistration::new(version, accounts);
        account_state.add_registrations(vec![registration]);

        // Should be able to schedule the reservation
        let result = account_state.try_schedule_next_reservation(&account_id, version);
        assert!(result.is_some());
        assert_eq!(account_state.last_known_min_balance, 500);
        assert_eq!(*account_state.reserved.get(&version).unwrap(), 500);

        // The registration should be completed
        assert_eq!(*receiver.borrow(), ScheduleResult::ReadyForExecution);
    }

    #[tokio::test]
    async fn test_account_state_insufficient_balance() {
        let account_id = ObjectID::random();
        let version = SequenceNumber::from_u64(1);
        let mut mock_balance_read = MockBalanceRead::new(version);
        mock_balance_read.set_balance(account_id, 100);

        let mut account_state = AccountState::new(&mock_balance_read, &account_id, version);

        // Add a registration with withdraw amount more than balance
        let mut accounts = BTreeMap::new();
        accounts.insert(account_id, 500);
        let (registration, receiver) = TxWithdrawRegistration::new(version, accounts);
        account_state.add_registrations(vec![registration]);

        // Should not be able to schedule the reservation
        let result = account_state.try_schedule_next_reservation(&account_id, version);
        assert!(result.is_none());
        assert_eq!(account_state.last_known_min_balance, 100);
        assert!(account_state.reserved.is_empty());

        // The registration should still be pending
        assert_eq!(*receiver.borrow(), ScheduleResult::Init);
    }

    #[tokio::test]
    async fn test_account_state_multi_account_registration() {
        let version = SequenceNumber::from_u64(1);
        let mut mock_balance_read = MockBalanceRead::new(version);

        // Set up multiple accounts
        let account1 = ObjectID::random();
        let account2 = ObjectID::random();
        mock_balance_read.set_balance(account1, 1000);
        mock_balance_read.set_balance(account2, 2000);

        // Create a registration that involves both accounts
        let mut accounts = BTreeMap::new();
        accounts.insert(account1, 500);
        accounts.insert(account2, 1500);
        let (registration, receiver) = TxWithdrawRegistration::new(version, accounts);

        // Test account1's state
        let mut account1_state = AccountState::new(&mock_balance_read, &account1, version);
        account1_state.add_registrations(vec![registration.clone()]);

        // Should be able to schedule the reservation for account1
        let result = account1_state.try_schedule_next_reservation(&account1, version);
        assert!(result.is_some());
        assert_eq!(account1_state.last_known_min_balance, 500); // 1000 - 500
        assert_eq!(*account1_state.reserved.get(&version).unwrap(), 500);

        // The registration should still be pending because account2 hasn't been processed
        assert_eq!(*receiver.borrow(), ScheduleResult::Init);

        // Test account2's state
        let mut account2_state = AccountState::new(&mock_balance_read, &account2, version);
        account2_state.add_registrations(vec![registration.clone()]);

        // Should be able to schedule the reservation for account2
        let result = account2_state.try_schedule_next_reservation(&account2, version);
        assert!(result.is_some());
        assert_eq!(account2_state.last_known_min_balance, 500); // 2000 - 1500
        assert_eq!(*account2_state.reserved.get(&version).unwrap(), 1500);

        // Now the registration should be complete since both accounts are processed
        assert_eq!(*receiver.borrow(), ScheduleResult::ReadyForExecution);

        // Test settlement
        account1_state.settle_accumulator_version(version);
        assert_eq!(account1_state.last_known_min_balance, 1000);
        assert!(account1_state.reserved.is_empty());

        account2_state.settle_accumulator_version(version);
        assert_eq!(account2_state.last_known_min_balance, 2000);
        assert!(account2_state.reserved.is_empty());
    }
}
