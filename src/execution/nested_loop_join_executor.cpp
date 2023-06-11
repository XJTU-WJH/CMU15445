//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_empty_(true),
      right_empty_(true),
      pos_(0),
      alaways_false_(false) {}

void NestedLoopJoinExecutor::Init() {
  if (plan_->predicate_ != nullptr) {
    if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(&(*(plan_->predicate_)));
        const_expr != nullptr) {
      alaways_false_ = !(const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>());
    }
  }
  if (alaways_false_) {
    return;
  }
  left_executor_->Init();
  right_executor_->Init();
  left_empty_ = true;
  left_empty_ = !(left_executor_->Next(&left_tuple_, &left_rid_));
  Tuple right_tuple;
  RID right_rid;
  right_empty_ = true;
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    rhs_.emplace_back(std::make_pair(right_tuple, right_rid));
    right_empty_ = false;
  }
  pos_ = 0;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (alaways_false_) {
    return false;
  }
  if (left_empty_) {
    return false;
  }
  if (right_empty_) {
    if (plan_->GetJoinType() == JoinType::INNER) {
      return false;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> res;
      auto col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
      for (uint32_t i = 0; i < col_cnt; i++) {
        res.emplace_back(left_tuple_.GetValue(&(left_executor_->GetOutputSchema()), i));
      }
      col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
      for (uint32_t i = 0; i < col_cnt; i++) {
        res.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(res, &(plan_->OutputSchema()));
      left_empty_ = !(left_executor_->Next(&left_tuple_, &left_rid_));
      pos_ = 0;
      return true;
    }
  }
  auto true_value = ValueFactory::GetBooleanValue(true);
  auto null_value = ValueFactory::GetBooleanValue(CmpBool::CmpNull);
  bool flag = false;
  if (pos_ == 0) {
    flag = true;
  }
  while (!left_empty_) {
    if (pos_ >= rhs_.size()) {
      flag = !flag;
      if (!flag && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> res;
        auto col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
        for (uint32_t i = 0; i < col_cnt; i++) {
          res.emplace_back(left_tuple_.GetValue(&(left_executor_->GetOutputSchema()), i));
        }
        col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
        for (uint32_t i = 0; i < col_cnt; i++) {
          res.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(res, &(plan_->OutputSchema()));
        left_empty_ = !(left_executor_->Next(&left_tuple_, &left_rid_));
        pos_ = 0;
        return true;
      }
      left_empty_ = !(left_executor_->Next(&left_tuple_, &left_rid_));
      pos_ = 0;
    } else {
      auto value = plan_->Predicate().EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                                   &(rhs_[pos_].first), plan_->GetRightPlan()->OutputSchema());
      if (value.CompareEquals(true_value) == CmpBool::CmpTrue) {
        std::vector<Value> res;
        auto col_cnt = left_executor_->GetOutputSchema().GetColumnCount();
        for (uint32_t i = 0; i < col_cnt; i++) {
          res.emplace_back(left_tuple_.GetValue(&(left_executor_->GetOutputSchema()), i));
        }
        col_cnt = right_executor_->GetOutputSchema().GetColumnCount();
        for (uint32_t i = 0; i < col_cnt; i++) {
          res.emplace_back(rhs_[pos_].first.GetValue(&(right_executor_->GetOutputSchema()), i));
        }
        *tuple = Tuple(res, &(plan_->OutputSchema()));
        pos_++;
        return true;
      }
      pos_++;
    }
  }
  return false;
}

}  // namespace bustub
