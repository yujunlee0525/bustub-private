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
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  empty_table_ = true;
  aht_.Clear();
  while (child_executor_->Next(&tuple, &rid)) {
    empty_table_ = false;
    auto key = MakeAggregateKey(&tuple);
    auto value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (empty_table_) {
      auto value = aht_.GenerateInitialAggregateValue().aggregates_;
      if (value.size() != plan_->OutputSchema().GetColumnCount()) {
        return false;
      }
      *tuple = Tuple(value, &plan_->OutputSchema());
      empty_table_ = false;
      return true;
    }
    return false;
  }
  std::vector<Value> value(aht_iterator_.Key().group_bys_);

  auto aggregates = aht_iterator_.Val().aggregates_;
  for (const auto &aggregate : aggregates) {
    value.push_back(aggregate);
  }
  *tuple = Tuple(value, &plan_->OutputSchema());
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
