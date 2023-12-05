#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
auto CheckExpressions(const bustub::AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> *left_expr,
                      std::vector<AbstractExpressionRef> *right_expr) -> bool {
  auto new_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (new_expr != nullptr) {
    if (new_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }
    auto left_child = new_expr->GetChildAt(0);
    auto right_child = new_expr->GetChildAt(1);
    auto left_column = dynamic_cast<const ColumnValueExpression *>(left_child.get());
    auto right_column = dynamic_cast<const ColumnValueExpression *>(right_child.get());
    if (left_column == nullptr || right_column == nullptr) {
      return false;
    }
    if (left_column->GetTupleIdx() == 0) {
      (*left_expr).emplace_back(left_child);
    } else {
      (*right_expr).emplace_back(left_child);
    }
    if (right_column->GetTupleIdx() == 0) {
      (*left_expr).emplace_back(right_child);
    } else {
      (*right_expr).emplace_back(right_child);
    }
    return true;
  }
  auto logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
  if (logic_expr != nullptr) {
    if (logic_expr->logic_type_ != LogicType::And) {
      return false;
    }
    return CheckExpressions(logic_expr->GetChildAt(0), left_expr, right_expr) &&
           CheckExpressions(logic_expr->GetChildAt(1), left_expr, right_expr);
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    // Check if expr is equal condition where one is for the left table, and one is for the right table.
    auto expr = nlj_plan.Predicate();
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    if (CheckExpressions(expr, &left_key_expressions, &right_key_expressions)) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left_key_expressions, right_key_expressions,
                                                nlj_plan.GetJoinType());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
