#include <algorithm>
#include <iostream>
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

// Note for 2022 Fall: You can add all optimizer rule implementations and apply the rules as you want in this file. Note
// that for some test cases, we force using starter rules, so that the configuration here won't take effects. Starter
// rule can be forcibly enabled by `set force_optimizer_starter_rule=yes`.

namespace bustub {

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizerElimateFalse(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizerInnerJoinPushDownAllAndFilter(p);
  p = OptimizerNLJAsFilterDown(p);
  p = OptimizeNLJAsIndexJoin(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  std::cout << "==================================" << std::endl;
  std::cout << p->ToString() << std::endl;
  std::cout << "==================================" << std::endl;
  return p;
}

auto Optimizer::JudgeAllAnd(const AbstractExpression &expr, std::vector<AbstractExpressionRef> &all_predicate) -> bool {
  // FIXME: 后续删除常数判断的部分，因为语法解析并不允许构建常数判断，所以不会出现该情况
  if (const auto *exptr = dynamic_cast<const ConstantValueExpression *>(&expr); exptr != nullptr) {
    all_predicate.push_back(std::make_shared<ConstantValueExpression>(*exptr));
    return true;
  }
  if (const auto *exptr = dynamic_cast<const ComparisonExpression *>(&expr); exptr != nullptr) {
    all_predicate.push_back(std::make_shared<ComparisonExpression>(*exptr));
    return true;
  }
  if (const auto *exptr = dynamic_cast<const LogicExpression *>(&expr); exptr != nullptr) {
    if (exptr->logic_type_ != LogicType::And) {
      return false;
    }
    bool left_flag = JudgeAllAnd(*(exptr->GetChildAt(0)), all_predicate);
    if (!left_flag) {
      return false;
    }
    bool right_flag = JudgeAllAnd(*(exptr->GetChildAt(1)), all_predicate);
    return left_flag && right_flag;
  }
  return false;
}

auto Optimizer::JudgeArithmeticType(const AbstractExpressionRef &predicate) -> int {
  int left_type = 0;
  int right_type = 0;
  const auto left = predicate->GetChildAt(0);
  const auto right = predicate->GetChildAt(1);
  if (const auto *exptr = dynamic_cast<const ColumnValueExpression *>(&(*left)); exptr != nullptr) {
    left_type = exptr->GetTupleIdx() + 1;
  }
  if (const auto *exptr = dynamic_cast<const ColumnValueExpression *>(&(*right)); exptr != nullptr) {
    right_type = exptr->GetTupleIdx() + 1;
  }
  if (left_type > 0 && right_type > 0) {
    if (left_type != right_type) {
      return 0;
    }
    return left_type;
  }
  return left_type > 0 ? left_type : right_type;
}

auto Optimizer::JudgePredicateType(const AbstractExpressionRef &exp) -> int {
  BUSTUB_ASSERT(exp->children_.size() == 2, "ComparisionExpression should only have 2 childs");
  const auto left_exp = exp->children_[0];
  const auto right_exp = exp->children_[1];
  int left_type = 0;
  int right_type = 0;
  if (const auto *exptr = dynamic_cast<const ColumnValueExpression *>(&(*left_exp)); exptr != nullptr) {
    left_type = exptr->GetTupleIdx() + 1;
  }
  if (const auto *exptr = dynamic_cast<const ArithmeticExpression *>(&(*left_exp)); exptr != nullptr) {
    left_type = JudgeArithmeticType(left_exp);
  }
  if (const auto *exptr = dynamic_cast<const ColumnValueExpression *>(&(*right_exp)); exptr != nullptr) {
    right_type = exptr->GetTupleIdx() + 1;
  }
  if (const auto *exptr = dynamic_cast<const ArithmeticExpression *>(&(*right_exp)); exptr != nullptr) {
    right_type = JudgeArithmeticType(right_exp);
  }
  if (left_type > 0 && right_type > 0) {
    if (left_type != right_type) {
      return 0;
    }
    return left_type;
  }
  return left_type > 0 ? left_type : right_type;
}

auto Optimizer::ReconstructChildNode(std::vector<AbstractExpressionRef> &predicate, const AbstractPlanNodeRef &child)
    -> AbstractPlanNodeRef {
  // FIXME: remove after debug
  {
    for (auto &it : predicate) {
      const auto *ptr = dynamic_cast<const ComparisonExpression *>(&(*it));
      if (ptr == nullptr) {
        throw std::logic_error("expression shoule be comparison expression in Optimizer::ReconstructChildNode");
      }
    }
  }
  if (child->GetType() == PlanType::SeqScan) {
    const auto *seq_plan = dynamic_cast<const SeqScanPlanNode *>(&(*child));
    if (predicate.size() == 1) {
      if (seq_plan->filter_predicate_ != nullptr) {
        predicate[0] = std::make_shared<LogicExpression>(predicate[0], seq_plan->filter_predicate_, LogicType::And);
      }
      return std::make_shared<SeqScanPlanNode>(seq_plan->output_schema_, seq_plan->GetTableOid(), seq_plan->table_name_,
                                               predicate[0]);
    }
    auto pre = std::make_shared<LogicExpression>(predicate[0], predicate[1], LogicType::And);
    for (size_t i = 2; i < predicate.size(); i++) {
      pre = std::make_shared<LogicExpression>(pre, predicate[i], LogicType::And);
    }
    if (seq_plan->filter_predicate_ != nullptr) {
      pre = std::make_shared<LogicExpression>(pre, seq_plan->filter_predicate_, LogicType::And);
    }
    return std::make_shared<SeqScanPlanNode>(seq_plan->output_schema_, seq_plan->GetTableOid(), seq_plan->table_name_,
                                             pre);
  }
  if (child->GetType() == PlanType::NestedLoopJoin) {
    const auto *nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode *>(&(*child));
    size_t left_cnt = nlj_plan->GetLeftPlan()->OutputSchema().GetColumnCount();
    size_t right_cnt = nlj_plan->GetRightPlan()->OutputSchema().GetColumnCount();
    if (predicate.size() == 1) {
      auto pre = RewriteExpressionForJoin(predicate[0], left_cnt, right_cnt);
      if (nlj_plan->predicate_ != nullptr) {
        pre = std::make_shared<LogicExpression>(pre, nlj_plan->predicate_, LogicType::And);
      }
      return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan->output_schema_, nlj_plan->GetLeftPlan(),
                                                      nlj_plan->GetRightPlan(), pre, nlj_plan->GetJoinType());
    }
    auto pre =
        std::make_shared<LogicExpression>(RewriteExpressionForJoin(predicate[0], left_cnt, right_cnt),
                                          RewriteExpressionForJoin(predicate[1], left_cnt, right_cnt), LogicType::And);
    for (size_t i = 2; i < predicate.size(); i++) {
      pre = std::make_shared<LogicExpression>(pre, RewriteExpressionForJoin(predicate[i], left_cnt, right_cnt),
                                              LogicType::And);
    }
    if (nlj_plan->predicate_ != nullptr) {
      pre = std::make_shared<LogicExpression>(pre, nlj_plan->predicate_, LogicType::And);
    }
    return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan->output_schema_, nlj_plan->GetLeftPlan(),
                                                    nlj_plan->GetRightPlan(), pre, nlj_plan->GetJoinType());
  }
  throw std::logic_error("unknown child type in Optimizer::ReconstructChildNode");
}

auto Optimizer::DecomposeFilter(std::vector<AbstractExpressionRef> &all_predicate,
                                std::vector<AbstractExpressionRef> &cur_child,
                                std::vector<AbstractExpressionRef> &left_child,
                                std::vector<AbstractExpressionRef> &right_child) -> bool {
  for (auto &exp : all_predicate) {
    if (const auto *exptr = dynamic_cast<const ConstantValueExpression *>(&(*exp)); exptr != nullptr) {
      bool flag = exptr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>();
      if (!flag) {
        return false;
      }
    } else if (const auto *exptr = dynamic_cast<const ComparisonExpression *>(&(*exp)); exptr != nullptr) {
      int type = JudgePredicateType(exp);
      switch (type) {
        case 0:
          cur_child.push_back(exp);
          break;
        case 1:
          left_child.push_back(exp);
          break;
        case 2:
          right_child.push_back(exp);
          break;
        default:
          throw std::logic_error("unknown predicate type in DecomposeFilter");
          break;
      }
    } else {
      throw std::logic_error("unsupport expression type in Optimizer::DecomposeFilter");
    }
  }
  return true;
}

auto Optimizer::OptimizerInnerJoinPushDownAllAndFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::Aggregation) {
    const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(*plan);
    return std::make_shared<AggregationPlanNode>(
        agg_plan.output_schema_, OptimizerInnerJoinPushDownAllAndFilter(agg_plan.GetChildPlan()),
        agg_plan.GetGroupBys(), agg_plan.GetAggregates(), agg_plan.GetAggregateTypes());
  }
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
    std::vector<AbstractExpressionRef> all_predicate;
    if (nlj_plan.GetJoinType() == JoinType::INNER && JudgeAllAnd(nlj_plan.Predicate(), all_predicate)) {
      std::vector<AbstractExpressionRef> cur_child;
      std::vector<AbstractExpressionRef> left_child;
      std::vector<AbstractExpressionRef> right_child;
      bool cur_flag = DecomposeFilter(all_predicate, cur_child, left_child, right_child);
      // std::cout<<"----------------cur_child---------------"<<std::endl;
      // for (auto &it: cur_child) {
      //   std::cout<<it->ToString()<<std::endl;
      // }
      // std::cout<<"----------------left_child---------------"<<std::endl;
      // for (auto &it: left_child) {
      //   std::cout<<it->ToString()<<std::endl;
      // }
      // std::cout<<"----------------right_child---------------"<<std::endl;
      // for (auto &it: right_child) {
      //   std::cout<<it->ToString()<<std::endl;
      // }
      if (!cur_flag) {
        auto false_value = ValueFactory::GetBooleanValue(false);
        auto false_predicate = std::make_shared<ConstantValueExpression>(false_value);
        return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                        nlj_plan.GetRightPlan(), false_predicate,
                                                        nlj_plan.GetJoinType());
      }
      if (left_child.empty() && right_child.empty()) {
        return plan;
      }
      AbstractPlanNodeRef left_node;
      AbstractPlanNodeRef right_node;
      if (nlj_plan.GetLeftPlan()->GetType() != PlanType::MockScan && !left_child.empty()) {
        left_node = ReconstructChildNode(left_child, nlj_plan.GetLeftPlan());
        left_node = OptimizerInnerJoinPushDownAllAndFilter(left_node);
      } else {
        cur_child.insert(cur_child.end(), left_child.begin(), left_child.end());
        left_node = nlj_plan.GetLeftPlan();
      }
      if (nlj_plan.GetRightPlan()->GetType() != PlanType::MockScan && !right_child.empty()) {
        right_node = ReconstructChildNode(right_child, nlj_plan.GetRightPlan());
        right_node = OptimizerInnerJoinPushDownAllAndFilter(right_node);
      } else {
        cur_child.insert(cur_child.end(), right_child.begin(), right_child.end());
        right_node = nlj_plan.GetRightPlan();
      }
      AbstractExpressionRef pre;
      if (cur_child.empty()) {
        auto true_value = ValueFactory::GetBooleanValue(true);
        pre = std::make_shared<ConstantValueExpression>(true_value);
      } else if (cur_child.size() == 1) {
        pre = cur_child[0];
      } else {
        pre = std::make_shared<LogicExpression>(cur_child[0], cur_child[1], LogicType::And);
        for (size_t i = 2; i < cur_child.size(); i++) {
          pre = std::make_shared<LogicExpression>(pre, cur_child[i], LogicType::And);
        }
      }
      return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, left_node, right_node, pre,
                                                      nlj_plan.GetJoinType());
    }
  }
  return plan;
}

auto Optimizer::ExtractKeyFromPreicate(AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> &child)
    -> bool {
  size_t pos = BUSTUB_INT32_MAX;
  for (size_t i = 0; i < child.size(); i++) {
    int type = JudgePredicateType(child[i]);
    if (type == 0) {
      const auto *ptr = dynamic_cast<const ComparisonExpression *>(&(*child[i]));
      if (ptr == nullptr || ptr->comp_type_ != ComparisonType::Equal) {
        continue;
      }
      pos = i;
      break;
    }
  }
  if (pos != BUSTUB_INT32_MAX) {
    predicate = child[pos];
    child.erase(child.begin() + pos);
    return true;
  }
  return false;
}

auto Optimizer::ReconstructPredicate(AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> &child)
    -> void {
  if (child.size() == 1) {
    predicate = child[0];
  } else {
    predicate = std::make_shared<LogicExpression>(child[0], child[1], LogicType::And);
    for (size_t i = 2; i < child.size(); i++) {
      predicate = std::make_shared<LogicExpression>(predicate, child[i], LogicType::And);
    }
  }
}

auto Optimizer::OptimizerNLJAsFilterUp(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizerNLJAsFilterUp(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.GetJoinType() == JoinType::INNER) {
      std::vector<AbstractExpressionRef> all_predicate;
      if (JudgeAllAnd(nlj_plan.Predicate(), all_predicate)) {
        AbstractExpressionRef predicate;
        if (ExtractKeyFromPreicate(predicate, all_predicate)) {
          AbstractExpressionRef new_predicate = nullptr;
          ReconstructPredicate(new_predicate, all_predicate);
          auto new_nlj_plan =
              std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                       nlj_plan.GetRightPlan(), predicate, nlj_plan.GetJoinType());
          return std::make_shared<FilterPlanNode>(nlj_plan.output_schema_, new_predicate, new_nlj_plan);
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::Optimizer::MergeFilter(AbstractPlanNodeRef &child, std::vector<AbstractExpressionRef> &predicate)
    -> void {
  if (predicate.empty()) {
    return;
  }
  AbstractExpressionRef pre;
  if (predicate.size() == 1) {
    pre = predicate[0];
  } else {
    pre = std::make_shared<LogicExpression>(predicate[0], predicate[1], LogicType::And);
    for (size_t i = 2; i < predicate.size(); i++) {
      pre = std::make_shared<LogicExpression>(pre, predicate[i], LogicType::And);
    }
  }
  if (child->GetType() == PlanType::NestedLoopJoin || child->GetType() == PlanType::MockScan) {
    child = std::make_shared<FilterPlanNode>(child->output_schema_, pre, child);
  } else if (child->GetType() == PlanType::SeqScan) {
    const auto *ptr = dynamic_cast<const SeqScanPlanNode *>(&(*child));
    if (ptr->filter_predicate_ != nullptr) {
      pre = std::make_shared<LogicExpression>(pre, ptr->filter_predicate_, LogicType::And);
    }
    child = std::make_shared<SeqScanPlanNode>(ptr->output_schema_, ptr->table_oid_, ptr->table_name_, pre);
  } else if (child->GetType() == PlanType::Filter) {
    const auto *ptr = dynamic_cast<const FilterPlanNode *>(&(*child));
    if (ptr->predicate_ != nullptr) {
      pre = std::make_shared<LogicExpression>(pre, ptr->predicate_, LogicType::And);
      child = std::make_shared<FilterPlanNode>(ptr->output_schema_, pre, ptr->GetChildPlan());
    }
  } else {
    throw std::logic_error("unsupported plan type in OptimizerNLJAsFilterDown");
  }
}

auto Optimizer::OptimizerNLJAsFilterDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizerNLJAsFilterDown(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.GetJoinType() == JoinType::INNER) {
      std::vector<AbstractExpressionRef> all_predicate;
      if (JudgeAllAnd(nlj_plan.Predicate(), all_predicate)) {
        std::vector<AbstractExpressionRef> cur_child;
        std::vector<AbstractExpressionRef> left_child;
        std::vector<AbstractExpressionRef> right_child;
        if (DecomposeFilter(all_predicate, cur_child, left_child, right_child)) {
          AbstractExpressionRef key;
          if (ExtractKeyFromPreicate(key, cur_child)) {
            AbstractPlanNodeRef left = nlj_plan.GetLeftPlan();
            AbstractPlanNodeRef right = nlj_plan.GetRightPlan();
            MergeFilter(left, left_child);
            MergeFilter(right, right_child);
            if (cur_child.empty()) {
              return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, left, right, key,
                                                              nlj_plan.GetJoinType());
            }
            AbstractExpressionRef pre = cur_child[0];
            for (size_t i = 1; i < cur_child.size(); i++) {
              pre = std::make_shared<LogicExpression>(pre, cur_child[i], LogicType::And);
            }
            auto plan = std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, left, right, key,
                                                                 nlj_plan.GetJoinType());
            return std::make_shared<FilterPlanNode>(nlj_plan.output_schema_, pre, plan);
          }
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::PredicateState(std::vector<AbstractExpressionRef> &predicate) -> int {
  std::vector<Column> columns;
  Tuple tuple;
  Schema schema(columns);
  for (auto &it : predicate) {
    if (const auto *exptr = dynamic_cast<const ComparisonExpression *>(&(*it)); exptr != nullptr) {
      const auto &left = exptr->GetChildAt(0);
      const auto &right = exptr->GetChildAt(1);
      const auto *left_exptr = dynamic_cast<const ConstantValueExpression *>(&(*left));
      const auto *right_exptr = dynamic_cast<const ConstantValueExpression *>(&(*right));
      if (left_exptr != nullptr && right_exptr != nullptr) {
        auto ret = exptr->Evaluate(&tuple, schema).GetAs<bool>();
        return static_cast<int>(ret);
      }
    }
  }
  return 2;
}

auto Optimizer::OptimizerElimateFalse(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizerElimateFalse(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);
    if (nlj_plan.GetJoinType() == JoinType::INNER) {
      std::vector<AbstractExpressionRef> all_predicate;
      if (JudgeAllAnd(nlj_plan.Predicate(), all_predicate)) {
        int ret = PredicateState(all_predicate);
        if (ret != 2) {
          AbstractExpressionRef pre;
          if (ret == 0) {
            auto false_value = ValueFactory::GetBooleanValue(false);
            pre = std::make_shared<ConstantValueExpression>(false_value);
          } else {
            auto true_value = ValueFactory::GetBooleanValue(true);
            pre = std::make_shared<ConstantValueExpression>(true_value);
          }
          nlj_plan.predicate_ = pre;
          return optimized_plan;
        }
      }
    }
  }
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    auto &seq_plan = dynamic_cast<SeqScanPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> all_predicate;
    if (JudgeAllAnd(*(seq_plan.filter_predicate_), all_predicate)) {
      int ret = PredicateState(all_predicate);
      if (ret != 2) {
        AbstractExpressionRef pre;
        if (ret == 0) {
          auto false_value = ValueFactory::GetBooleanValue(false);
          pre = std::make_shared<ConstantValueExpression>(false_value);
        } else {
          auto true_value = ValueFactory::GetBooleanValue(true);
          pre = std::make_shared<ConstantValueExpression>(true_value);
        }
        seq_plan.filter_predicate_ = pre;
        return optimized_plan;
      }
    }
  }
  if (optimized_plan->GetType() == PlanType::Filter) {
    auto &fil_plan = dynamic_cast<FilterPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> all_predicate;
    if (JudgeAllAnd(*(fil_plan.predicate_), all_predicate)) {
      int ret = PredicateState(all_predicate);
      if (ret != 2) {
        AbstractExpressionRef pre;
        if (ret == 0) {
          auto false_value = ValueFactory::GetBooleanValue(false);
          pre = std::make_shared<ConstantValueExpression>(false_value);
        } else {
          auto true_value = ValueFactory::GetBooleanValue(true);
          pre = std::make_shared<ConstantValueExpression>(true_value);
        }
        fil_plan.predicate_ = pre;
        if (ret == 0) {
          auto child = fil_plan.GetChildPlan();
          if (child->GetType() == PlanType::NestedLoopJoin) {
            auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*child);
            return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                            nlj_plan.GetRightPlan(), pre, nlj_plan.GetJoinType());
          }
          if (child->GetType() == PlanType::SeqScan) {
            auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*child);
            return std::make_shared<SeqScanPlanNode>(seq_plan.output_schema_, seq_plan.table_oid_, seq_plan.table_name_,
                                                     pre);
          }
        }
        return optimized_plan;
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizerMergeProjection(AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef { return plan; }

}  // namespace bustub
