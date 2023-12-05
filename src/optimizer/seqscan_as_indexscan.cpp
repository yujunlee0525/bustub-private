#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeOrderByAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    auto pred = seq_scan_plan.filter_predicate_;
    // no filtering: seqscan
    if (pred == nullptr) {
      return optimized_plan;
    }
    const auto *compare_expr = dynamic_cast<ComparisonExpression *>(pred.get());
    // check if it is comparison = statement else return seqscan
    if (compare_expr == nullptr) {
      return optimized_plan;
    }
    if (compare_expr->comp_type_ != ComparisonType::Equal) {
      return optimized_plan;
    }
    // get table and indices
    const auto *table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
    const auto indices = catalog_.GetTableIndexes(table_info->name_);
    auto count = 0;
    std::vector<uint32_t> col_idx;
    // there should only be one column value expression
    for (auto &child_expr : compare_expr->GetChildren()) {
      const auto *column_expr = dynamic_cast<ColumnValueExpression *>(child_expr.get());
      if (column_expr != nullptr) {
        col_idx.push_back(column_expr->GetColIdx());
        count += 1;
      }
    }

    if (count != 1) {
      return optimized_plan;
    }
    auto *value_expr = dynamic_cast<ConstantValueExpression *>(compare_expr->GetChildAt(1).get());
    // make index scan plan node
    for (const auto *index : indices) {
      const auto &columns = index->index_->GetKeyAttrs();
      if (col_idx == columns) {
        AbstractPlanNodeRef new_plan =
            std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_, index->index_oid_,
                                                seq_scan_plan.filter_predicate_, value_expr);
        return new_plan;
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
