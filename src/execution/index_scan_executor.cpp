//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), index_info_(nullptr) {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  auto tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  index_begin_ = tree->GetBeginIterator();
  index_end_ = tree->GetEndIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_begin_ == index_end_) {
    return false;
  }
  auto val = *(index_begin_);
  bool result = table_info_->table_->GetTuple(val.second, tuple, exec_ctx_->GetTransaction());
  if (!result) {
    throw std::logic_error("index scan failed!");
  }
  ++index_begin_;
  return true;
}

}  // namespace bustub
