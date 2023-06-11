//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), insert_finish_(false) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  insert_finish_ = false;
  if (!(exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                               plan_->TableOid()))) {
    std::cout << "transaction id: " << exec_ctx_->GetTransaction()->GetTransactionId() << " Table IX LOCK failed"
              << std::endl;
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw ExecutionException("error");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (insert_finish_) {
    return false;
  }
  insert_finish_ = true;
  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto index = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);
  int insert_cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    bool result = table->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    if (!result) {
      throw std::logic_error("insert failed!");
    }
    for (auto it : index) {
      Tuple key_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), it->key_schema_,
                                            it->index_->GetMetadata()->GetKeyAttrs());
      it->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    insert_cnt++;
    if (!(exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                               plan_->TableOid(), *rid))) {
      std::cout << "transaction id: " << exec_ctx_->GetTransaction()->GetTransactionId() << " Row X LOCK failed"
                << std::endl;
      exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
      throw ExecutionException("error");
    }
  }
  std::vector<Value> result{Value(INTEGER, insert_cnt)};
  *tuple = Tuple(result, &(plan_->OutputSchema()));
  return true;
}

}  // namespace bustub
