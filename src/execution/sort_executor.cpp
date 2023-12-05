#include "execution/executors/sort_executor.h"
#include <algorithm>

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  tuples_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(), [this](Tuple &a, Tuple &b) {
    auto plan = this->plan_;
    auto schema = this->child_executor_->GetOutputSchema();
    for (auto &order_by : plan->GetOrderBy()) {
      auto order_type = order_by.first;
      auto expr = order_by.second;
      auto first_value = expr->Evaluate(&a, schema);
      auto second_value = expr->Evaluate(&b, schema);
      if (first_value.CompareGreaterThan(second_value) == CmpBool::CmpTrue) {
        return order_type == OrderByType::DESC;
      }
      if (first_value.CompareLessThan(second_value) == CmpBool::CmpTrue) {
        return order_type != OrderByType::DESC;
      }
    }
    return false;
  });
  iterator_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ >= tuples_.size()) {
    return false;
  }
  *tuple = tuples_[iterator_];
  iterator_++;
  return true;
}

}  // namespace bustub
