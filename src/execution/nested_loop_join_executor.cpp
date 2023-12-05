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
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/expressions/column_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  no_match_ = true;
  left_executor_->Init();
  right_executor_->Init();
  RID rid;
  done_ = !left_executor_->Next(&tuple_, &rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple r_tuple;
  RID r_rid;
  auto predicate = plan_->Predicate();
  while (!done_) {
    if (right_executor_->Next(&r_tuple, &r_rid)) {
      auto same = predicate->EvaluateJoin(&tuple_, left_executor_->GetOutputSchema(), &r_tuple,
                                          right_executor_->GetOutputSchema());
      if (same.GetAs<bool>()) {
        std::vector<Value> value;
        auto left_schema = left_executor_->GetOutputSchema();
        auto right_schema = right_executor_->GetOutputSchema();
        for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
          value.push_back(tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
          value.push_back(r_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(value, &GetOutputSchema());
        no_match_ = false;
        return true;
      }
    } else {
      if (no_match_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> value;
        auto left_schema = left_executor_->GetOutputSchema();
        auto right_schema = right_executor_->GetOutputSchema();
        for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
          value.push_back(tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
          value.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
        }
        *tuple = Tuple(value, &GetOutputSchema());
        done_ = !left_executor_->Next(&tuple_, rid);
        right_executor_->Init();
        return true;
      }
      no_match_ = true;
      done_ = !left_executor_->Next(&tuple_, rid);
      right_executor_->Init();
    }
  }
  return false;
}
}  // namespace bustub
