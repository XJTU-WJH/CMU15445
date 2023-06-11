#include "execution/executors/filter_executor.h"
#include "common/exception.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

FilterExecutor::FilterExecutor(ExecutorContext *exec_ctx, const FilterPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), alaways_false_(false) {}

void FilterExecutor::Init() {
  // Initialize the child executor
  if (plan_->GetPredicate() != nullptr) {
    if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(&(*(plan_->GetPredicate())));
        const_expr != nullptr) {
      alaways_false_ = !(const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>());
    }
  }
  if (alaways_false_) {
    return;
  }
  child_executor_->Init();
}

auto FilterExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (alaways_false_) {
    return false;
  }
  auto filter_expr = plan_->GetPredicate();

  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(tuple, rid);

    if (!status) {
      return false;
    }

    auto value = filter_expr->Evaluate(tuple, child_executor_->GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      return true;
    }
  }
}

}  // namespace bustub
