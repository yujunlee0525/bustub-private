//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  all_updated_ = false;
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (all_updated_) {
    return false;
  }
  int32_t row = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_heap = table_info_->table_.get();
  auto indices = catalog->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(tuple, rid)) {
    // delete old tuple
    auto old_tuple_meta = TupleMeta();
    old_tuple_meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(old_tuple_meta, *rid);
    // insert new Tuple
    auto tuple_meta = TupleMeta();
    tuple_meta.is_deleted_ = false;
    std::vector<Value> values{};
    values.reserve(table_info_->schema_.GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple{values, &table_info_->schema_};
    auto new_rid = table_heap->InsertTuple(tuple_meta, new_tuple);
    // update indices
    for (auto index_info : indices) {
      auto key =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      auto old_key =
          (*tuple).KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, new_rid.value(), exec_ctx_->GetTransaction());
      index_info->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
    }
    row++;
  }
  std::vector<Value> val;
  std::vector<Column> col;
  val.emplace_back(INTEGER, row);
  col.emplace_back("Updated_Rows", INTEGER);
  auto schema = Schema(col);
  *tuple = Tuple(val, &schema);
  all_updated_ = true;
  return true;
}

}  // namespace bustub
