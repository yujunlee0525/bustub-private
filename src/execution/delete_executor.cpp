//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  all_deleted_ = false;
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (all_deleted_) {
    return false;
  }
  int32_t row = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  auto table_heap = table_info->table_.get();
  auto indices = catalog->GetTableIndexes(table_info->name_);
  while (child_executor_->Next(tuple, rid)) {
    // delete old tuple
    auto old_tuple_meta = TupleMeta();
    old_tuple_meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(old_tuple_meta, *rid);
    for (auto index_info : indices) {
      auto key = (*tuple).KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    row++;
  }
  std::vector<Value> val;
  std::vector<Column> col;
  val.emplace_back(INTEGER, row);
  col.emplace_back("Updated_Rows", INTEGER);
  auto schema = Schema(col);
  *tuple = Tuple(val, &schema);
  all_deleted_ = true;
  return true;
}

}  // namespace bustub
