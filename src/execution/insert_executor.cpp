//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  no_more_tuples_ = false;
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (no_more_tuples_) {
    return false;
  }
  int32_t row = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_temp = catalog->GetTable(plan_->GetTableOid());
  auto table_heap = table_temp->table_.get();
  auto indices = catalog->GetTableIndexes(table_temp->name_);

  while (child_executor_->Next(tuple, rid)) {
    auto tuple_meta = TupleMeta();
    tuple_meta.is_deleted_ = false;
    auto new_rid = table_heap->InsertTuple(tuple_meta, *tuple);
    for (auto index_info : indices) {
      auto key = (*tuple).KeyFromTuple(table_temp->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, new_rid.value(), exec_ctx_->GetTransaction());
    }
    row++;
  }
  std::vector<Value> val;
  std::vector<Column> col;
  val.emplace_back(INTEGER, row);
  col.emplace_back("Inserted_Rows", INTEGER);
  auto schema = Schema(col);
  *tuple = Tuple(val, &schema);
  no_more_tuples_ = true;
  return true;
}

}  // namespace bustub
