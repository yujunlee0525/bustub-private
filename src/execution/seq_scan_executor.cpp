//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_temp = catalog->GetTable(plan_->GetTableOid());
  auto table_heap = table_temp->table_.get();
  table_iterator_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  bool filter;
  if (table_iterator_->IsEnd()) {
    return false;
  }
  auto tuple_meta = table_iterator_->GetTuple().first;
  *tuple = table_iterator_->GetTuple().second;
  if (plan_->filter_predicate_ == nullptr) {
    filter = true;
  } else {
    filter = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema()).GetAs<bool>();
  }
  while (!filter || tuple_meta.is_deleted_) {
    ++(*table_iterator_);
    if (table_iterator_->IsEnd()) {
      return false;
    }
    tuple_meta = table_iterator_->GetTuple().first;
    *tuple = table_iterator_->GetTuple().second;
    if (plan_->filter_predicate_ != nullptr) {
      filter = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema()).GetAs<bool>();
    }
  }
  *rid = table_iterator_->GetRID();
  ++(*table_iterator_);
  return true;
}

}  // namespace bustub
