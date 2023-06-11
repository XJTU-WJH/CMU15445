//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), delete_finish_(false) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  delete_finish_ = false;
  std::cout << "transaction id: " << exec_ctx_->GetTransaction()->GetTransactionId()
            << " table id: " << plan_->table_oid_
            << "IsolationLevel: " << static_cast<int>(exec_ctx_->GetTransaction()->GetIsolationLevel())
            << " Table IX LOCK" << std::endl;
  if (!(exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                               plan_->TableOid()))) {
    std::cout << "transaction id: " << exec_ctx_->GetTransaction()->GetTransactionId() << " Table IX LOCK failed"
              << std::endl;
    exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    throw ExecutionException("error");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (delete_finish_) {
    return false;
  }
  delete_finish_ = true;
  auto table = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto index = exec_ctx_->GetCatalog()->GetTableIndexes(table->name_);
  int delete_cnt = 0;
  while (child_executor_->Next(tuple, rid)) {
    if (!(exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                               plan_->TableOid(), *rid))) {
      std::cout << "transaction id: " << exec_ctx_->GetTransaction()->GetTransactionId() << " Row X LOCK failed"
                << std::endl;
      exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
      throw ExecutionException("error");
    }
    bool result = table->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    if (!result) {
      throw std::logic_error("delete failed!");
    }
    // TableWriteRecord tr(*rid, WType::DELETE, *tuple, &(*(table->table_)));
    // exec_ctx_->GetTransaction()->AppendTableWriteRecord(tr);
    for (auto it : index) {
      Tuple key_tuple = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), it->key_schema_,
                                            it->index_->GetMetadata()->GetKeyAttrs());
      it->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
      // IndexWriteRecord ir(*rid, plan_->TableOid(), WType::DELETE, key_tuple, it->index_oid_,
      // exec_ctx_->GetCatalog()); exec_ctx_->GetTransaction()->AppendIndexWriteRecord(ir);
    }
    delete_cnt++;
  }
  std::vector<Value> result{Value(INTEGER, delete_cnt)};
  *tuple = Tuple(result, &(plan_->OutputSchema()));
  return true;
}

}  // namespace bustub
