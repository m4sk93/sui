// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    digests::TransactionDigest,
};

pub(crate) mod account_state;
pub(crate) mod balance_read;
pub(crate) mod scheduler;

#[cfg(test)]
mod tests;

/// The result of scheduling the withdraw reservations for a transaction.
#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub(crate) enum ScheduleResult {
    /// The withdraw reservation has not been processed yet.
    /// This is the initial value of the watch channel for monitoring the result.
    Init,
    /// The transaction in question has already been executed.
    /// No need to schedule the withdraw reservations.
    AlreadyExecuted,
    /// We have reached a deterministic state where the transaction can be executed.
    /// Either because we know for sure there is enough balance to satisfy all withdraw reservations
    /// in this transaction, or because the previous dependent settlement transaction
    /// has been executed and we have the latest prior state.
    ReadyForExecution,
}

/// Details regarding a balance settlement, generated when a settlement
/// transaction has been executed and committed to the writeback cache.
#[allow(dead_code)]
pub(crate) struct BalanceSettlement {
    pub old_accumulator_version: SequenceNumber,
    pub new_accumulator_version: SequenceNumber,
    pub balance_changes: BTreeMap<ObjectID, i128>,
}

/// Details regarding all balance withdraw reservations in a transaction.
#[allow(dead_code)]
pub(crate) struct TxBalanceWithdraw {
    pub tx_digest: TransactionDigest,
    pub reservations: BTreeMap<ObjectID, u64>,
}
