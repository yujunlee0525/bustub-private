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
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  ht_iterator_ = 0;
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  ht_.clear();
  left_child_->Init();
  right_child_->Init();
  Tuple tuple;
  RID rid;
  // build phase with right child
  while (right_child_->Next(&tuple, &rid)) {
    HashJoinKey rkey;
    auto right_expr = plan_->RightJoinKeyExpressions();
    std::vector<Value> rgroups;
    rgroups.reserve(right_expr.size());
    for (auto &expr : right_expr) {
      rgroups.push_back(expr->Evaluate(&tuple, right_child_->GetOutputSchema()));
    }
    rkey.group_bys_ = rgroups;
    if (ht_.count(rkey) != 0) {
      ht_[rkey].push_back(tuple);
    } else {
      ht_[rkey] = std::vector{tuple};
    }
  }
  // probe phase
  while (left_child_->Next(&tuple, &rid)) {
    HashJoinKey key;
    auto left_expr = plan_->LeftJoinKeyExpressions();
    std::vector<Value> groups;
    groups.reserve(left_expr.size());
    for (auto &expr : left_expr) {
      groups.push_back(expr->Evaluate(&tuple, left_child_->GetOutputSchema()));
    }
    key.group_bys_ = groups;
    if (ht_.count(key) != 0) {
      auto right_tuples = ht_.find(key)->second;
      for (auto &right_tuple : right_tuples) {
        std::vector<Value> value;
        auto left_schema = left_child_->GetOutputSchema();
        auto right_schema = right_child_->GetOutputSchema();
        for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
          value.push_back(tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
          value.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
        }
        output_.emplace_back(Tuple(value, &GetOutputSchema()));
      }
    } else {  // Left JOIN
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> value;
        auto left_schema = left_child_->GetOutputSchema();
        auto right_schema = right_child_->GetOutputSchema();
        for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
          value.push_back(tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
          value.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
        }
        output_.emplace_back(Tuple(value, &GetOutputSchema()));
      }
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ht_iterator_ >= output_.size()) {
    return false;
  }
  *tuple = output_[ht_iterator_];
  ht_iterator_++;
  return true;
}

}  // namespace bustub
