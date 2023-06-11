//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(nullptr),
      index_(nullptr) {}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  index_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    bool result = child_executor_->Next(&left_tuple_, &left_rid_);
    if (!result) {
      break;
    }
    std::vector<Value> val;
    val.emplace_back(plan_->KeyPredicate()->Evaluate(&left_tuple_, child_executor_->GetOutputSchema()));
    std::vector<RID> res;
    index_->index_->ScanKey(Tuple(val, &(index_->key_schema_)), &res, exec_ctx_->GetTransaction());
    if (res.empty()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> ret;
        auto col_cnt = child_executor_->GetOutputSchema().GetColumnCount();
        for (uint32_t i = 0; i < col_cnt; i++) {
          ret.emplace_back(left_tuple_.GetValue(&(child_executor_->GetOutputSchema()), i));
        }
        col_cnt = plan_->InnerTableSchema().GetColumnCount();
        for (uint32_t i = 0; i < col_cnt; i++) {
          ret.emplace_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(ret, &(plan_->OutputSchema()));
        return true;
      }
      continue;
    }
    result = table_info_->table_->GetTuple(res[0], &right_tuple_, exec_ctx_->GetTransaction());
    if (!result) {
      throw std::logic_error("Couldn't find tuple by RID in NestIndexJoinExecutor::Next");
    }
    std::vector<Value> ret;
    auto col_cnt = child_executor_->GetOutputSchema().GetColumnCount();
    for (uint32_t i = 0; i < col_cnt; i++) {
      ret.emplace_back(left_tuple_.GetValue(&(child_executor_->GetOutputSchema()), i));
    }
    col_cnt = plan_->InnerTableSchema().GetColumnCount();
    for (uint32_t i = 0; i < col_cnt; i++) {
      ret.emplace_back(right_tuple_.GetValue(&(plan_->InnerTableSchema()), i));
    }
    *tuple = Tuple(ret, &(plan_->OutputSchema()));
    return true;
  }
  return false;
}

}  // namespace bustub
