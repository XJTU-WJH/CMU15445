//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::GetTableLockMode(Transaction *txn, const table_oid_t &oid, bool &flag) -> LockMode {
  flag = true;
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    return LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    return LockMode::INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return LockMode::INTENTION_SHARED;
  }
  if (txn->IsTableExclusiveLocked(oid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsTableSharedLocked(oid)) {
    return LockMode::SHARED;
  }
  flag = false;
  return LockMode::SHARED;
}

auto LockManager::HoldTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
    default:
      break;
  }
}

auto LockManager::CheckUpgradeTableLock(LockMode origin, LockMode new_mode) -> bool {
  switch (origin) {
    case LockMode::INTENTION_SHARED:
      if (new_mode == LockMode::SHARED || new_mode == LockMode::EXCLUSIVE ||
          new_mode == LockMode::INTENTION_EXCLUSIVE || new_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (new_mode == LockMode::EXCLUSIVE || new_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (new_mode == LockMode::EXCLUSIVE) {
        return true;
      }
      break;
    default:
      break;
  }
  return false;
}

auto LockManager::UpgradeTableLock(Transaction *txn, LockMode origin_mode, LockMode new_mode, const table_oid_t &oid,
                                   bool is_delete) -> void {
  if (is_delete) {
    switch (origin_mode) {
      case LockMode::SHARED:
        txn->GetSharedTableLockSet()->erase(oid);
        break;
      case LockMode::EXCLUSIVE:
        txn->GetExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_SHARED:
        txn->GetIntentionSharedTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
        break;
      default:
        break;
    }
  } else {
    switch (new_mode) {
      case LockMode::SHARED:
        txn->GetSharedTableLockSet()->insert(oid);
        break;
      case LockMode::EXCLUSIVE:
        txn->GetExclusiveTableLockSet()->insert(oid);
        break;
      case LockMode::INTENTION_SHARED:
        txn->GetIntentionSharedTableLockSet()->insert(oid);
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn->GetIntentionExclusiveTableLockSet()->insert(oid);
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
        break;
      default:
        break;
    }
  }
}

auto LockManager::IsolationLevelCheck(Transaction *txn, LockMode lock_mode, AbortReason &abort) -> bool {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        abort = AbortReason::LOCK_ON_SHRINKING;
        return false;
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED &&
          lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        abort = AbortReason::LOCK_ON_SHRINKING;
        return false;
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        abort = AbortReason::LOCK_ON_SHRINKING;
        return false;
      }
      if ((lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE)) {
        txn->SetState(TransactionState::ABORTED);
        abort = AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED;
        return false;
      }
      break;
    default:
      break;
  }
  return true;
}

auto LockManager::CheckTableLockCompatible(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  for (auto it : table_lock_map_[oid]->request_queue_) {
    if (it->granted_ && it->oid_ == oid && it->txn_id_ != txn->GetTransactionId()) {
      switch (it->lock_mode_) {
        case LockMode::SHARED:
          if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lock_mode == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::EXCLUSIVE:
          return false;
          break;
        case LockMode::INTENTION_SHARED:
          if (lock_mode == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          if (lock_mode == LockMode::SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
              lock_mode == LockMode::EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          if (lock_mode != LockMode::INTENTION_SHARED) {
            return false;
          }
          break;
        default:
          break;
      }
    }
  }
  return true;
}

auto LockManager::RemoveTransationFromTable(Transaction *txn, const table_oid_t &oid) -> void {
  if (table_lock_map_[oid] == nullptr) {
    return;
  }
  for (auto it = table_lock_map_[oid]->request_queue_.begin(); it != table_lock_map_[oid]->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      switch ((*it)->lock_mode_) {
        case LockMode::SHARED:
          txn->GetSharedTableLockSet()->erase(oid);
          break;
        case LockMode::EXCLUSIVE:
          txn->GetExclusiveTableLockSet()->erase(oid);
          break;
        case LockMode::INTENTION_SHARED:
          txn->GetIntentionSharedTableLockSet()->erase(oid);
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          txn->GetIntentionExclusiveTableLockSet()->erase(oid);
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
          break;
        default:
          break;
      }
      auto *req = *it;
      table_lock_map_[oid]->request_queue_.erase(it);
      delete req;
      if (table_lock_map_[oid]->upgrading_ == txn->GetTransactionId()) {
        table_lock_map_[oid]->upgrading_ = INVALID_TXN_ID;
      }
      table_lock_map_[oid]->cv_.notify_all();
      break;
    }
  }
}

auto LockManager::IsPriorityTableLock(Transaction *txn, const table_oid_t &oid) -> bool {
  for (auto it : table_lock_map_[oid]->request_queue_) {
    if (!(it->granted_)) {
      return it->txn_id_ == txn->GetTransactionId();
    }
  }
  return true;
}

auto LockManager::IsPriorityrowLock(Transaction *txn, const RID &rid) -> bool {
  for (auto it : row_lock_map_[rid]->request_queue_) {
    if (!(it->granted_)) {
      return it->txn_id_ == txn->GetTransactionId();
    }
  }
  return true;
}

auto LockManager::RemoveAllRowLockFromTable(Transaction *txn, const table_oid_t &oid) -> void {
  auto &s = (*(txn->GetSharedRowLockSet()))[oid];
  while (s.begin() != s.end()) {
    auto it = s.begin();
    if (row_lock_map_[*it] == nullptr) {
      continue;
    }
    std::scoped_lock<std::mutex> lck(row_lock_map_[*it]->latch_);
    for (auto p = row_lock_map_[*it]->request_queue_.begin(); p != row_lock_map_[*it]->request_queue_.end(); p++) {
      if ((*p)->txn_id_ == txn->GetTransactionId()) {
        delete *p;
        row_lock_map_[*it]->request_queue_.erase(p);
        row_lock_map_[*it]->cv_.notify_all();
        break;
      }
    }
    s.erase(it);
  }
  auto &x = (*(txn->GetExclusiveRowLockSet()))[oid];
  while (x.begin() != x.end()) {
    auto it = x.begin();
    if (row_lock_map_[*it] == nullptr) {
      continue;
    }
    std::scoped_lock<std::mutex> lck(row_lock_map_[*it]->latch_);
    for (auto p = row_lock_map_[*it]->request_queue_.begin(); p != row_lock_map_[*it]->request_queue_.end(); p++) {
      if ((*p)->txn_id_ == txn->GetTransactionId()) {
        delete *p;
        row_lock_map_[*it]->request_queue_.erase(p);
        row_lock_map_[*it]->cv_.notify_all();
        break;
      }
    }
    x.erase(it);
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::cout << "LockTable, "
            << "transaction id: " << txn->GetTransactionId() << " LockMode: " << static_cast<int>(lock_mode)
            << " IsolationLevel: " << static_cast<int>(txn->GetIsolationLevel())
            << " state:" << static_cast<int>(txn->GetState()) << " table_id: " << oid << std::endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    std::scoped_lock<std::mutex> lck(table_lock_map_[oid]->latch_);
    RemoveTransationFromTable(txn, oid);
    return false;
  }
  AbortReason abort;
  bool result = IsolationLevelCheck(txn, lock_mode, abort);
  if (!result) {
    std::cout << "IsolationLevelCheck failed... "
              << "transaction id: " << txn->GetTransactionId() << " LockMode: " << static_cast<int>(lock_mode)
              << " IsolationLevel: " << static_cast<int>(txn->GetIsolationLevel())
              << " state:" << static_cast<int>(txn->GetState()) << " table_id: " << oid << std::endl;
    if (table_lock_map_[oid] != nullptr) {
      std::scoped_lock<std::mutex> lck(table_lock_map_[oid]->latch_);
      RemoveTransationFromTable(txn, oid);
    }
    throw TransactionAbortException(txn->GetTransactionId(), abort);
    return false;
  }
  bool flag = true;
  LockMode mode = GetTableLockMode(txn, oid, flag);
  if (flag) {
    if (mode == lock_mode) {
      return true;
    }
    std::unique_lock<std::mutex> lck(table_lock_map_[oid]->latch_);
    if (!CheckUpgradeTableLock(mode, lock_mode)) {
      RemoveTransationFromTable(txn, oid);
      txn->SetState(TransactionState::ABORTED);
      std::cout << "INCOMPATIBLE_UPGRADE, "
                << "transaction id: " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
    if (table_lock_map_[oid]->upgrading_ != INVALID_TXN_ID &&
        table_lock_map_[oid]->upgrading_ != txn->GetTransactionId()) {
      RemoveAllRowLockFromTable(txn, oid);
      RemoveTransationFromTable(txn, oid);
      txn->SetState(TransactionState::ABORTED);
      std::cout << "UPGRADE_CONFLICT, "
                << "transaction id: " << txn->GetTransactionId() << std::endl;
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    table_lock_map_[oid]->upgrading_ = txn->GetTransactionId();
    UpgradeTableLock(txn, mode, lock_mode, oid, true);
    LockRequest *req = nullptr;
    for (auto it = table_lock_map_[oid]->request_queue_.begin(); it != table_lock_map_[oid]->request_queue_.end();
         it++) {
      if ((*it)->txn_id_ == txn->GetTransactionId()) {
        req = *it;
        table_lock_map_[oid]->request_queue_.erase(it);
        break;
      }
    }
    while (true) {
      if (txn->GetState() == TransactionState::ABORTED) {
        delete req;
        if (table_lock_map_[oid]->upgrading_ == txn->GetTransactionId()) {
          table_lock_map_[oid]->upgrading_ = INVALID_TXN_ID;
        }
        table_lock_map_[oid]->cv_.notify_all();
        return false;
      }
      if (CheckTableLockCompatible(txn, lock_mode, oid)) {
        if (req != nullptr) {
          req->lock_mode_ = lock_mode;
          table_lock_map_[oid]->request_queue_.push_back(req);
        }
        UpgradeTableLock(txn, mode, lock_mode, oid);
        table_lock_map_[oid]->upgrading_ = INVALID_TXN_ID;
        table_lock_map_[oid]->cv_.notify_all();
        return true;
      }
      std::cout << "upgrade wait..., "
                << "transaction id: " << txn->GetTransactionId() << " LockMode: " << static_cast<int>(lock_mode)
                << " IsolationLevel: " << static_cast<int>(txn->GetIsolationLevel())
                << " state:" << static_cast<int>(txn->GetState()) << " table_id: " << oid << std::endl;
      table_lock_map_[oid]->cv_.wait(lck);
      std::cout << "upgrade get lock..., "
                << "transaction id: " << txn->GetTransactionId() << " LockMode: " << static_cast<int>(lock_mode)
                << " IsolationLevel: " << static_cast<int>(txn->GetIsolationLevel())
                << " state:" << static_cast<int>(txn->GetState()) << " table_id: " << oid << std::endl;
    }
    return true;
  }
  auto *req = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  {
    std::scoped_lock<std::mutex> lck(table_lock_map_latch_);
    if (table_lock_map_[oid] == nullptr) {
      table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    }
  }
  std::unique_lock<std::mutex> lck(table_lock_map_[oid]->latch_);
  table_lock_map_[oid]->request_queue_.push_back(req);
  while (true) {
    if (txn->GetState() == TransactionState::ABORTED) {
      for (auto it = table_lock_map_[oid]->request_queue_.begin(); it != table_lock_map_[oid]->request_queue_.end();
           it++) {
        if ((*it)->txn_id_ == txn->GetTransactionId()) {
          table_lock_map_[oid]->request_queue_.erase(it);
          break;
        }
      }
      delete req;
      table_lock_map_[oid]->cv_.notify_all();
      return false;
    }
    if (IsPriorityTableLock(txn, oid) && table_lock_map_[oid]->upgrading_ == INVALID_TXN_ID &&
        CheckTableLockCompatible(txn, lock_mode, oid)) {
      req->granted_ = true;
      HoldTableLock(txn, lock_mode, oid);
      table_lock_map_[oid]->cv_.notify_all();
      return true;
    }
    std::cout << "wait..., "
              << "transaction id: " << txn->GetTransactionId() << " LockMode: " << static_cast<int>(lock_mode)
              << " state:" << static_cast<int>(txn->GetState()) << " table_id: " << oid << std::endl;
    table_lock_map_[oid]->cv_.wait(lck);
    std::cout << "get lock..., "
              << "transaction id: " << txn->GetTransactionId() << " LockMode: " << static_cast<int>(lock_mode)
              << " state:" << static_cast<int>(txn->GetState()) << " table_id: " << oid << std::endl;
  }
  return false;
}

auto LockManager::FindTableLockMode(Transaction *txn, const table_oid_t &oid, bool &is_find) -> LockMode {
  is_find = true;
  if (txn->IsTableSharedLocked(oid)) {
    return LockMode::SHARED;
  }
  if (txn->IsTableExclusiveLocked(oid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return LockMode::INTENTION_SHARED;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    return LockMode::INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    return LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  is_find = false;
  return LockMode::SHARED;
}

auto LockManager::DeleteTableLockMode(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    default:
      break;
  }
  for (auto it = table_lock_map_[oid]->request_queue_.begin(); it != table_lock_map_[oid]->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      delete *it;
      table_lock_map_[oid]->request_queue_.erase(it);
      return;
    }
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::cout << "UnlockTable, "
            << "transaction id: " << txn->GetTransactionId() << " State: " << static_cast<int>(txn->GetState())
            << " IsolationLevel: " << static_cast<int>(txn->GetIsolationLevel()) << " table_id: " << oid << std::endl;
  bool is_find = false;
  if (txn->GetState() == TransactionState::ABORTED) {
    std::unique_lock<std::mutex> lck(table_lock_map_[oid]->latch_);
    RemoveTransationFromTable(txn, oid);
    return is_find;
  }
  LockMode mode = FindTableLockMode(txn, oid, is_find);
  if (!is_find) {
    std::cout << "ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, "
              << "transaction id: " << txn->GetTransactionId() << std::endl;
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  if (!(*(txn->GetSharedRowLockSet()))[oid].empty() || !(*(txn->GetExclusiveRowLockSet()))[oid].empty()) {
    std::cout << "TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS, "
              << "transaction id: " << txn->GetTransactionId() << std::endl;
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::GROWING && (mode == LockMode::SHARED || mode == LockMode::EXCLUSIVE)) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::GROWING && mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn->GetState() == TransactionState::GROWING && mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      if (mode == LockMode::SHARED) {
        throw std::logic_error("S locks are not permitted under READ_UNCOMMITTED");
      }
      break;
    default:
      break;
  }
  {
    std::scoped_lock<std::mutex> lck(table_lock_map_[oid]->latch_);
    DeleteTableLockMode(txn, mode, oid);
  }
  std::cout << "notify... "
            << "transaction id: " << txn->GetTransactionId() << " state:" << static_cast<int>(txn->GetState())
            << " table_id: " << oid << std::endl;
  table_lock_map_[oid]->cv_.notify_all();
  return true;
}

auto LockManager::GetRowLockMode(Transaction *txn, const table_oid_t &oid, const RID &rid, bool &flag) -> LockMode {
  flag = true;
  if (txn->IsRowSharedLocked(oid, rid)) {
    return LockMode::SHARED;
  }
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    return LockMode::EXCLUSIVE;
  }
  flag = false;
  return LockMode::SHARED;
}

auto LockManager::UpgradeRowLock(Transaction *txn, LockMode origin_mode, LockMode new_mode, const table_oid_t &oid,
                                 const RID &rid, bool is_delete) -> void {
  if (is_delete) {
    switch (origin_mode) {
      case LockMode::SHARED:
        (*(txn->GetSharedRowLockSet()))[oid].erase(rid);
        break;
      case LockMode::EXCLUSIVE:
        (*(txn->GetExclusiveRowLockSet()))[oid].erase(rid);
        break;
      default:
        break;
    }
  } else {
    switch (new_mode) {
      case LockMode::SHARED:
        (*(txn->GetSharedRowLockSet()))[oid].insert(rid);
        break;
      case LockMode::EXCLUSIVE:
        (*(txn->GetExclusiveRowLockSet()))[oid].insert(rid);
        break;
      default:
        break;
    }
  }
}

auto LockManager::CheckRowLockCompatible(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid)
    -> bool {
  return std::all_of(row_lock_map_[rid]->request_queue_.begin(), row_lock_map_[rid]->request_queue_.end(),
                     [lock_mode](LockRequest *it) {
                       if (it == nullptr) {
                         return true;
                       }
                       if (it->granted_) {
                         if (it->lock_mode_ == LockMode::EXCLUSIVE) {
                           return false;
                         }
                         if (it->lock_mode_ == LockMode::SHARED && lock_mode == LockMode::EXCLUSIVE) {
                           return false;
                         }
                       }
                       return true;
                     });
}

auto LockManager::HoldRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> void {
  switch (lock_mode) {
    case LockMode::SHARED:
      (*(txn->GetSharedRowLockSet()))[oid].insert(rid);
      break;
    case LockMode::EXCLUSIVE:
      (*(txn->GetExclusiveRowLockSet()))[oid].insert(rid);
      break;
    default:
      break;
  }
}

auto LockManager::RemoveTransationFromRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> void {
  if (row_lock_map_[rid] == nullptr) {
    return;
  }
  for (auto it = row_lock_map_[rid]->request_queue_.begin(); it != row_lock_map_[rid]->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      auto table_id = (*it)->oid_;
      switch ((*it)->lock_mode_) {
        case LockMode::SHARED:
          (*(txn->GetSharedRowLockSet()))[table_id].erase(rid);
          break;
        case LockMode::EXCLUSIVE:
          (*(txn->GetExclusiveRowLockSet()))[table_id].erase(rid);
          break;
        default:
          break;
      }
      auto *req = *it;
      row_lock_map_[rid]->request_queue_.erase(it);
      delete req;
      if (row_lock_map_[rid]->upgrading_ == txn->GetTransactionId()) {
        row_lock_map_[rid]->upgrading_ = INVALID_TXN_ID;
      }
      row_lock_map_[rid]->cv_.notify_all();
      break;
    }
  }
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // std::cout << "LockRow "
  //           << "transaction id: " << txn->GetTransactionId() << " State: " << static_cast<int>(txn->GetState())
  //           << " IsolationLevel: " << static_cast<int>(txn->GetIsolationLevel())
  //           << " lock_mode: " << static_cast<int>(lock_mode) << " table_id: " << oid << " RID: " << rid << std::endl;

  if (txn->GetState() == TransactionState::ABORTED) {
    {
      std::lock_guard<std::mutex> lck(row_lock_map_[rid]->latch_);
      RemoveTransationFromRow(txn, oid, rid);
    }
    {
      std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
      RemoveTransationFromTable(txn, oid);
    }
    return false;
  }
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    {
      std::lock_guard<std::mutex> lck(row_lock_map_[rid]->latch_);
      RemoveTransationFromRow(txn, oid, rid);
    }
    {
      std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
      RemoveTransationFromTable(txn, oid);
    }
    std::cout << "ATTEMPTED_INTENTION_LOCK_ON_ROW, "
              << "transaction id: " << txn->GetTransactionId() << std::endl;
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }
  AbortReason abort;
  bool result = IsolationLevelCheck(txn, lock_mode, abort);
  if (!result) {
    if (row_lock_map_[rid] != nullptr) {
      std::lock_guard<std::mutex> lck(row_lock_map_[rid]->latch_);
      RemoveTransationFromRow(txn, oid, rid);
    }
    if (table_lock_map_[oid] != nullptr) {
      std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
      RemoveTransationFromTable(txn, oid);
    }
    std::cout << "IsolationLevelCheck, "
              << "transaction id: " << txn->GetTransactionId() << std::endl;
    throw TransactionAbortException(txn->GetTransactionId(), abort);
    return false;
  }
  bool flag = true;
  LockMode mode = GetRowLockMode(txn, oid, rid, flag);
  if (flag) {
    LockMode table_mode = GetTableLockMode(txn, oid, flag);
    if (!flag || (lock_mode == LockMode::EXCLUSIVE && table_mode != LockMode::EXCLUSIVE &&
                  table_mode != LockMode::INTENTION_EXCLUSIVE && table_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
      std::cout << "TABLE_LOCK_NOT_PRESENT "
                << "transaction id: " << txn->GetTransactionId() << " State: " << static_cast<int>(txn->GetState())
                << " IsolationLevel: " << static_cast<int>(txn->GetIsolationLevel()) << " table_id: " << oid
                << " RID: " << rid << std::endl;
      {
        std::lock_guard<std::mutex> lck(row_lock_map_[rid]->latch_);
        RemoveTransationFromRow(txn, oid, rid);
      }
      {
        std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
        RemoveTransationFromTable(txn, oid);
      }
      std::cout << "TABLE_LOCK_NOT_PRESENT, "
                << "transaction id: " << txn->GetTransactionId() << std::endl;
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
    if (mode == lock_mode) {
      return true;
    }
    if (!(mode == LockMode::SHARED || mode == LockMode::EXCLUSIVE)) {
      if (row_lock_map_[rid] != nullptr) {
        std::lock_guard<std::mutex> lck(row_lock_map_[rid]->latch_);
        RemoveTransationFromRow(txn, oid, rid);
      }
      if (table_lock_map_[oid] != nullptr) {
        std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
        RemoveTransationFromTable(txn, oid);
      }
      std::cout << "INCOMPATIBLE_UPGRADE, "
                << "transaction id: " << txn->GetTransactionId() << std::endl;
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
    std::unique_lock<std::mutex> lck(row_lock_map_[rid]->latch_);
    if (row_lock_map_[rid]->upgrading_ != INVALID_TXN_ID && row_lock_map_[rid]->upgrading_ != txn->GetTransactionId()) {
      RemoveTransationFromRow(txn, oid, rid);
      {
        std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
        RemoveTransationFromTable(txn, oid);
      }
      std::cout << "UPGRADE_CONFLICT, "
                << "transaction id: " << txn->GetTransactionId() << std::endl;
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    row_lock_map_[rid]->upgrading_ = txn->GetTransactionId();
    UpgradeRowLock(txn, mode, lock_mode, oid, rid, true);
    LockRequest *req = nullptr;
    for (auto it = row_lock_map_[rid]->request_queue_.begin(); it != row_lock_map_[rid]->request_queue_.end(); it++) {
      if ((*it)->txn_id_ == txn->GetTransactionId()) {
        req = *it;
        row_lock_map_[rid]->request_queue_.erase(it);
        break;
      }
    }
    while (true) {
      if (txn->GetState() == TransactionState::ABORTED) {
        delete req;
        if (row_lock_map_[rid]->upgrading_ == txn->GetTransactionId()) {
          row_lock_map_[rid]->upgrading_ = INVALID_TXN_ID;
        }
        row_lock_map_[rid]->cv_.notify_all();
        return false;
      }
      if (CheckRowLockCompatible(txn, lock_mode, oid, rid)) {
        if (req != nullptr) {
          req->lock_mode_ = lock_mode;
          row_lock_map_[rid]->request_queue_.push_back(req);
        }
        UpgradeRowLock(txn, mode, lock_mode, oid, rid);
        row_lock_map_[rid]->cv_.notify_all();
        return true;
      }
      std::cout << "row wait..., "
                << "transaction id: " << txn->GetTransactionId() << " LockMode: " << static_cast<int>(lock_mode)
                << " state:" << static_cast<int>(txn->GetState()) << " table_id: " << oid << std::endl;
      row_lock_map_[rid]->cv_.wait(lck);
      std::cout << "row get_lock..., "
                << "transaction id: " << txn->GetTransactionId() << " LockMode: " << static_cast<int>(lock_mode)
                << " state:" << static_cast<int>(txn->GetState()) << " table_id: " << oid << std::endl;
    }
    return true;
  }
  LockMode table_mode = GetTableLockMode(txn, oid, flag);
  if (!flag || (lock_mode == LockMode::EXCLUSIVE && table_mode != LockMode::EXCLUSIVE &&
                table_mode != LockMode::INTENTION_EXCLUSIVE && table_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    std::cout << "TABLE_LOCK_NOT_PRESENT "
              << "transaction id: " << txn->GetTransactionId() << " State: " << static_cast<int>(txn->GetState())
              << " IsolationLevel: " << static_cast<int>(txn->GetIsolationLevel()) << " table_id: " << oid
              << " RID: " << rid << std::endl;
    {
      std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
      RemoveTransationFromTable(txn, oid);
    }
    std::cout << "TABLE_LOCK_NOT_PRESENT, "
              << "transaction id: " << txn->GetTransactionId() << std::endl;
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }
  {
    std::scoped_lock<std::mutex> lck(row_lock_map_latch_);
    if (row_lock_map_[rid] == nullptr) {
      row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    }
  }
  auto *req = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  std::unique_lock<std::mutex> lck(row_lock_map_[rid]->latch_);
  row_lock_map_[rid]->request_queue_.push_back(req);
  while (true) {
    if (txn->GetState() == TransactionState::ABORTED) {
      std::cout << "Abort... "
                << "transaction id: " << txn->GetTransactionId() << std::endl;
      RemoveTransationFromRow(txn, oid, rid);
      if (table_lock_map_[oid] != nullptr) {
        std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
        RemoveTransationFromTable(txn, oid);
      }
      row_lock_map_[rid]->cv_.notify_all();
      return false;
    }
    if (IsPriorityrowLock(txn, rid) && row_lock_map_[rid]->upgrading_ == INVALID_TXN_ID &&
        CheckRowLockCompatible(txn, lock_mode, oid, rid)) {
      req->granted_ = true;
      HoldRowLock(txn, lock_mode, oid, rid);
      row_lock_map_[rid]->cv_.notify_all();
      return true;
    }
    std::cout << "row wait... "
              << "transaction id: " << txn->GetTransactionId() << std::endl;
    row_lock_map_[rid]->cv_.wait(lck);
    std::cout << "row get_lock... "
              << "transaction id: " << txn->GetTransactionId() << std::endl;
  }
  return false;
}

auto LockManager::FindRowLockMode(Transaction *txn, const table_oid_t &oid, const RID &rid, bool &is_find) -> LockMode {
  is_find = true;
  if (txn->IsRowSharedLocked(oid, rid)) {
    return LockMode::SHARED;
  }
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    return LockMode::EXCLUSIVE;
  }
  is_find = false;
  return LockMode::SHARED;
}

auto LockManager::DeleteRowLockMode(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid)
    -> void {
  switch (lock_mode) {
    case LockMode::SHARED:
      (*(txn->GetSharedRowLockSet()))[oid].erase(rid);
      break;
    case LockMode::EXCLUSIVE:
      (*(txn->GetExclusiveRowLockSet()))[oid].erase(rid);
      break;
    default:
      break;
  }
  for (auto it = row_lock_map_[rid]->request_queue_.begin(); it != row_lock_map_[rid]->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      delete *it;
      row_lock_map_[rid]->request_queue_.erase(it);
      return;
    }
  }
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // std::cout << "UnlockRow "
  //           << "transaction id: " << txn->GetTransactionId() << " State: " << static_cast<int>(txn->GetState())
  //           << " IsolationLevel: " << static_cast<int>(txn->GetIsolationLevel()) << " table_id: " << oid
  //           << " RID: " << rid << std::endl;
  if (txn->GetState() == TransactionState::ABORTED) {
    {
      std::lock_guard<std::mutex> lck(row_lock_map_[rid]->latch_);
      RemoveTransationFromRow(txn, oid, rid);
    }
    {
      std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
      RemoveTransationFromTable(txn, oid);
    }
    return false;
  }
  bool is_find = true;
  LockMode mode = FindRowLockMode(txn, oid, rid, is_find);
  if (!is_find) {
    {
      std::lock_guard<std::mutex> lck(table_lock_map_[oid]->latch_);
      RemoveTransationFromTable(txn, oid);
    }
    std::cout << "ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD, "
              << "transaction id: " << txn->GetTransactionId() << std::endl;
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::GROWING && (mode == LockMode::SHARED || mode == LockMode::EXCLUSIVE)) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::GROWING && mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn->GetState() == TransactionState::GROWING && mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      if (mode == LockMode::SHARED) {
        throw std::logic_error("S locks are not permitted under READ_UNCOMMITTED");
      }
      break;
    default:
      break;
  }
  {
    std::scoped_lock<std::mutex> lck(row_lock_map_[rid]->latch_);
    DeleteRowLockMode(txn, mode, oid, rid);
  }
  row_lock_map_[rid]->cv_.notify_all();
  return true;
}

auto LockManager::CheckAbort(txn_id_t txn) -> bool {
  if (TransactionManager::txn_map.find(txn) == TransactionManager::txn_map.end()) {
    return false;
  }
  auto *txn_id = TransactionManager::GetTransaction(txn);
  return txn_id->GetState() == TransactionState::ABORTED;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::scoped_lock<std::mutex> lck(waits_for_latch_);
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); it++) {
    if ((*it) == t2) {
      return;
    }
    if ((*it) > t2) {
      waits_for_[t1].insert(it, t2);
    }
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::scoped_lock<std::mutex> lck(waits_for_latch_);
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); it++) {
    if ((*it) == t2) {
      waits_for_[t1].erase(it);
      return;
    }
  }
}

auto LockManager::DfsFindCycle(txn_id_t cur, txn_id_t min_id, txn_id_t &txn, bool (&book)[1024]) -> bool {
  for (auto it : waits_for_[cur]) {
    if (CheckAbort(it)) {
      continue;
    }
    if (book[it]) {
      txn = min_id;
      return true;
    }
    book[it] = true;
    if (DfsFindCycle(it, std::max(it, min_id), txn, book)) {
      return true;
    }
    book[it] = false;
  }
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // if (TransactionManager::txn_map.begin() == TransactionManager::txn_map.end()) {
  //   std::cout << "txn_map is empty!" << std::endl;
  //   return false;
  // }
  {
    std::scoped_lock<std::mutex> lck(table_lock_map_latch_);
    for (auto &pa : table_lock_map_) {
      if (table_lock_map_[pa.first] != nullptr) {
        ConstructTableEdge(pa.first);
      }
    }
  }
  {
    std::scoped_lock<std::mutex> lck(row_lock_map_latch_);
    for (auto &pa : row_lock_map_) {
      if (row_lock_map_[pa.first] != nullptr) {
        ConstructRowEdge(pa.first);
      }
    }
  }
  // auto res = GetEdgeList();
  // std::cout << "==============begin===============" << std::endl;
  // for (auto &pa : res) {
  //   std::cout << pa.first << " -> " << pa.second << std::endl;
  // }
  bool book[1024] = {false};
  std::vector<txn_id_t> txn_vec;
  for (auto &pa : waits_for_) {
    txn_vec.push_back(pa.first);
  }
  sort(txn_vec.begin(), txn_vec.end());
  for (auto it : txn_vec) {
    if (CheckAbort(it)) {
      continue;
    }
    book[it] = true;
    if (DfsFindCycle(it, it, *txn_id, book)) {
      if (TransactionManager::txn_map.find(*txn_id) != TransactionManager::txn_map.end()) {
        auto *txn_p = TransactionManager::GetTransaction(*txn_id);
        txn_p->SetState(TransactionState::ABORTED);
      }
      return true;
    }
    book[it] = false;
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &pa : waits_for_) {
    auto t1 = pa.first;
    for (auto id : pa.second) {
      edges.emplace_back(std::make_pair(t1, id));
    }
  }
  return edges;
}

auto LockManager::ConstructTableEdge(table_oid_t tid) -> void {
  std::vector<txn_id_t> from;
  std::vector<txn_id_t> to;
  {
    std::scoped_lock<std::mutex> lck(table_lock_map_[tid]->latch_);
    // std::cout << "ConstructTableEdge, oid: " << tid << std::endl;
    for (auto &it : table_lock_map_[tid]->request_queue_) {
      // auto *txn_p = TransactionManager::GetTransaction(it->txn_id_);
      // if (txn_p->GetState() == TransactionState::ABORTED) {
      //   continue;
      // }
      if (it->granted_) {
        to.push_back(it->txn_id_);
      } else {
        from.push_back(it->txn_id_);
      }
    }
  }
  // std::cout << "release construct lock, oid: " << tid << std::endl;
  for (auto from_id : from) {
    for (auto to_id : to) {
      AddEdge(from_id, to_id);
    }
  }
}

auto LockManager::ConstructRowEdge(const RID &rid) -> void {
  std::vector<txn_id_t> from;
  std::vector<txn_id_t> to;
  {
    std::scoped_lock<std::mutex> lck(row_lock_map_[rid]->latch_);
    for (auto &it : row_lock_map_[rid]->request_queue_) {
      // auto *txn_p = TransactionManager::GetTransaction(it->txn_id_);
      // if (txn_p->GetState() == TransactionState::ABORTED) {
      //   continue;
      // }
      if (it->granted_) {
        to.push_back(it->txn_id_);
      } else {
        from.push_back(it->txn_id_);
      }
    }
  }
  for (auto from_id : from) {
    for (auto to_id : to) {
      AddEdge(from_id, to_id);
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      // auto res = GetEdgeList();
      // std::cout<<"==============begin==============="<<std::endl;
      // for (auto &pa: res) {
      //   std::cout<<pa.first<<" -> "<<pa.second<<std::endl;
      // }
      txn_id_t txn;
      while (HasCycle(&txn)) {
        // std::cout << "================================>>>>>>>>>>>>>>>>>>>>>>>>>>>find: " << txn << std::endl;
        bool flag = false;
        {
          std::scoped_lock<std::mutex> lck(row_lock_map_latch_);
          for (auto &pa : row_lock_map_) {
            {
              // std::cout<<"foreach-------------"<<std::endl;
              if (pa.second == nullptr) {
                continue;
              }
              std::scoped_lock<std::mutex> lck((pa.second)->latch_);
              for (auto que : (pa.second)->request_queue_) {
                // std::cout<<"txn: "<<txn<<" que txn_id: "<<que->txn_id_<<" RID: "<<pa.first<<" grant?
                // "<<que->granted_<<std::endl;
                if (que->txn_id_ == txn && !que->granted_) {
                  flag = true;
                  break;
                }
              }
            }
            if (flag) {
              // std::cout<<"notify for transaction: "<<txn<<" RID: "<<pa.first<<std::endl;
              row_lock_map_[pa.first]->cv_.notify_all();
              break;
            }
          }
        }
        if (flag) {
          continue;
        }
        {
          std::scoped_lock<std::mutex> lck(table_lock_map_latch_);
          for (auto &pa : table_lock_map_) {
            {
              if (pa.second == nullptr) {
                continue;
              }
              std::scoped_lock<std::mutex> lck((pa.second)->latch_);
              for (auto que : (pa.second)->request_queue_) {
                if (que->txn_id_ == txn && !que->granted_) {
                  flag = true;
                  break;
                }
              }
            }
            if (flag) {
              // std::cout<<"notify for transaction: "<<txn<<std::endl;
              table_lock_map_[pa.first]->cv_.notify_all();
              break;
            }
          }
        }
      }
      waits_for_.clear();
    }
  }
}

}  // namespace bustub
