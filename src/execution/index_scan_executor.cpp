//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  found_ = false;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  for (auto index : indexes) {
    if (index->index_oid_ == plan_->index_oid_) {
      index_info_ = index;
    }
  }
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (found_) {
    return false;
  }
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto table_heap = table_info->table_.get();

  std::vector<Value> value;
  value.push_back(plan_->pred_key_->val_);
  *tuple = Tuple(value, &index_info_->key_schema_);
  std::vector<RID> r;
  htable_->ScanKey(*tuple, &r, exec_ctx_->GetTransaction());
  if (r.empty()) {
    return false;
  }
  *rid = r.front();
  *tuple = table_heap->GetTuple(*rid).second;
  std::cout << tuple->ToString(&this->GetOutputSchema()) << std::endl;
  if (table_heap->GetTuple(*rid).first.is_deleted_) {
    return false;
  }
  found_ = true;
  return true;
}

}  // namespace bustub
