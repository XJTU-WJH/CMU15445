#include "execution/executors/sort_executor.h"
#include "type/value_factory.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), pos_(0) {}

void SortExecutor::Init() {
  child_executor_->Init();
  tuple_.clear();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuple_.emplace_back(std::make_pair(tuple, rid));
  }
  std::sort(tuple_.begin(), tuple_.end(), [this](std::pair<Tuple, RID> &lhs, std::pair<Tuple, RID> &rhs) -> bool {
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
  });
  pos_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (pos_ == tuple_.size()) {
    return false;
  }
  *tuple = tuple_[pos_].first;
  *rid = tuple_[pos_].second;
  ++pos_;
  return true;
}

}  // namespace bustub
