//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/hash_join_executor.h"
#include <iostream>
#include "murmur3/MurmurHash3.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      left_pos_(0),
      right_pos_(0) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  left_hash_.clear();
  right_hash_.clear();
  Tuple tuple;
  RID rid;
  while (left_child_->Next(&tuple, &rid)) {
    auto val = plan_->LeftJoinKeyExpression().Evaluate(&tuple, plan_->GetLeftPlan()->OutputSchema());
    uint64_t key = Hash(val.ToString());
    left_hash_[key].emplace_back(std::make_pair(tuple, rid));
  }
  while (right_child_->Next(&tuple, &rid)) {
    auto val = plan_->RightJoinKeyExpression().Evaluate(&tuple, plan_->GetRightPlan()->OutputSchema());
    uint64_t key = Hash(val.ToString());
    right_hash_[key].emplace_back(std::make_pair(tuple, rid));
  }
  left_iterator_ = left_hash_.begin();
  left_pos_ = 0;
  right_pos_ = 0;
}

auto HashJoinExecutor::Hash(const std::string &key) -> uint64_t {
  uint64_t hash[2];
  murmur3::MurmurHash3_x64_128(reinterpret_cast<const void *>(key.c_str()), static_cast<int>(key.size()), 0,
                               reinterpret_cast<void *>(&hash));
  return hash[0];
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_iterator_ != left_hash_.end()) {
    if (left_pos_ >= left_iterator_->second.size()) {
      left_iterator_++;
      left_pos_ = 0;
      right_pos_ = 0;
      continue;
    }
    auto &vec = right_hash_[left_iterator_->first];
    if (vec.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> res;
      auto col_cnt = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
      for (uint32_t i = 0; i < col_cnt; i++) {
        res.emplace_back(left_iterator_->second[left_pos_].first.GetValue(&(plan_->GetLeftPlan()->OutputSchema()), i));
      }
      col_cnt = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
      for (uint32_t i = 0; i < col_cnt; i++) {
        res.emplace_back(
            ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(res, &(plan_->OutputSchema()));
      left_pos_++;
      return true;
    }
    if (right_pos_ >= vec.size()) {
      if (left_pos_ >= left_iterator_->second.size()) {
        left_iterator_++;
        left_pos_ = 0;
      } else {
        left_pos_++;
      }
      right_pos_ = 0;
    } else {
      auto left_val = plan_->LeftJoinKeyExpression().Evaluate(&(left_iterator_->second[left_pos_].first),
                                                              plan_->GetLeftPlan()->OutputSchema());
      auto right_val =
          plan_->RightJoinKeyExpression().Evaluate(&(vec[right_pos_].first), plan_->GetRightPlan()->OutputSchema());
      if (left_val.CompareEquals(right_val) == CmpBool::CmpTrue) {
        std::vector<Value> res;
        auto col_cnt = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
        for (uint32_t i = 0; i < col_cnt; i++) {
          res.emplace_back(
              left_iterator_->second[left_pos_].first.GetValue(&(plan_->GetLeftPlan()->OutputSchema()), i));
        }
        col_cnt = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
        for (uint32_t i = 0; i < col_cnt; i++) {
          res.emplace_back(vec[right_pos_].first.GetValue(&(plan_->GetRightPlan()->OutputSchema()), i));
        }
        *tuple = Tuple(res, &(plan_->OutputSchema()));
        right_pos_++;
        return true;
      }
      right_pos_++;
    }
  }
  return false;
}

}  // namespace bustub
