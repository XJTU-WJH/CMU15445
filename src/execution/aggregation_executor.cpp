//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), empty_table_(false) {}

void AggregationExecutor::Init() {
  child_->Init();
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
  Tuple tuple;
  RID rid;
  empty_table_ = true;
  while (child_->Next(&tuple, &rid)) {
    empty_table_ = false;
    aht_->InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_beg_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
  aht_iterator_end_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->End());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (empty_table_) {
    if (plan_->GetAggregateTypes().size() != plan_->OutputSchema().GetColumnCount()) {
      return false;
    }
    std::vector<Value> res = aht_->GenerateEmptyTableValue().aggregates_;
    *tuple = Tuple(res, &(plan_->OutputSchema()));
    empty_table_ = false;
    return true;
  }
  if ((*aht_iterator_beg_) == (*aht_iterator_end_)) {
    return false;
  }
  AggregateKey key = aht_iterator_beg_->Key();
  AggregateValue val = aht_iterator_beg_->Val();
  std::vector<Value> res = key.group_bys_;
  res.insert(res.end(), val.aggregates_.begin(), val.aggregates_.end());
  *tuple = Tuple(res, &(plan_->OutputSchema()));
  aht_iterator_beg_->operator++();
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
