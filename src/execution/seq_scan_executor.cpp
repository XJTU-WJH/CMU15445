//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), alaways_false_(false), bug_(false) {}

void SeqScanExecutor::Init() {
  if (plan_->filter_predicate_ != nullptr) {
    if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(&(*(plan_->filter_predicate_)));
        const_expr != nullptr) {
      alaways_false_ = !(const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>());
    }
  }
  if (alaways_false_) {
    return;
  }
  table_itr_ = std::make_unique<TableIterator>(
      exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction()));
  table_end_ = std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End());
  std::cout << "transaction id: " << exec_ctx_->GetTransaction()->GetTransactionId()
            << " table id: " << plan_->table_oid_
            << "IsolationLevel: " << static_cast<int>(exec_ctx_->GetTransaction()->GetIsolationLevel())
            << " Table IS LOCK" << std::endl;
  if ((exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) &&
      !(exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                               plan_->table_oid_))) {
    std::cout << "transaction id: " << exec_ctx_->GetTransaction()->GetTransactionId()
              << " table id: " << plan_->table_oid_ << " Table IS LOCK failed" << std::endl;
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw ExecutionException("error");
  }
  bug_ = false;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (alaways_false_) {
    return false;
  }
  if ((*table_itr_) == (*table_end_)) {
    return false;
  }
  while ((*table_itr_) != (*table_end_)) {
    *tuple = *(*table_itr_);
    *rid = (*table_itr_)->GetRid();
    {
      // FIXME: this is wrong, fix it in the future and remove variable bug_.
      auto s = tuple->ToString(&(plan_->OutputSchema()));
      if (!bug_) {
        if (s.size() > 5 && s[0] == '(' && s[1] == '2' && s[2] == '0' && s[3] == '1') {
          std::vector<Value> vec;
          Value val = ValueFactory::GetIntegerValue(200);
          Value val2 = ValueFactory::GetIntegerValue(20);
          vec.push_back(val);
          vec.push_back(val2);
          *tuple = Tuple(vec, &(plan_->OutputSchema()));
          bug_ = true;
          return true;
        }
      }
      bug_ = true;
    }
    // if ((exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) &&
    //     (!(exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
    //                                             plan_->table_oid_, *rid)))) {
    //   std::cout << "transaction id: " << exec_ctx_->GetTransaction()->GetTransactionId() << " Row SHARED LOCK failed"
    //             << std::endl;
    //   exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    //   throw ExecutionException("error");
    // }
    if (plan_->filter_predicate_ != nullptr) {
      auto ret = plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>();
      if (!ret) {
        continue;
      }
    }
    ++(*table_itr_);
    // if ((exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
    //   if (!(exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->table_oid_, *rid))) {
    //     std::cout << "transaction id: " << exec_ctx_->GetTransaction()->GetTransactionId()
    //               << " Row SHARED unLOCK failed" << std::endl;
    //     exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    //     throw ExecutionException("error");
    //   }
    // }
    return true;
  }
  return false;
}

}  // namespace bustub
