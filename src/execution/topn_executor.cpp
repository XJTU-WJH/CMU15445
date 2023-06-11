#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  tuple_.clear();
  Tuple tuple;
  RID rid;
  auto cmp = [this](std::pair<Tuple, RID> &lhs, std::pair<Tuple, RID> &rhs) -> bool {
    for (auto &it : this->plan_->GetOrderBy()) {
      Value lhs_val = it.second->Evaluate(&(lhs.first), this->GetOutputSchema());
      Value rhs_val = it.second->Evaluate(&(rhs.first), this->GetOutputSchema());
      if (lhs_val.CompareEquals(rhs_val) == CmpBool::CmpTrue) {
        continue;
      }
      if (lhs_val.CompareGreaterThan(rhs_val) == CmpBool::CmpTrue) {
        if (it.first == OrderByType::DESC) {
          return true;
        }
        if (it.first == OrderByType::ASC || it.first == OrderByType::DEFAULT) {
          return false;
        }
      } else {
        if (it.first == OrderByType::DESC) {
          return false;
        }
        if (it.first == OrderByType::ASC || it.first == OrderByType::DEFAULT) {
          return true;
        }
      }
    }
    return true;
  };
  while (child_executor_->Next(&tuple, &rid)) {
    auto pa = std::make_pair(tuple, rid);
    auto it = tuple_.begin();
    while (it != tuple_.end()) {
      bool result = cmp(pa, *it);
      if (result) {
        break;
      }
      it++;
    }
    if (tuple_.size() == plan_->GetN()) {
      if (it != tuple_.end()) {
        tuple_.insert(it, pa);
        tuple_.pop_back();
      }
    } else {
      tuple_.insert(it, pa);
    }
  }
}
auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuple_.empty()) {
    return false;
  }
  *tuple = tuple_.begin()->first;
  *rid = tuple_.begin()->second;
  tuple_.pop_front();
  return true;
}

}  // namespace bustub
