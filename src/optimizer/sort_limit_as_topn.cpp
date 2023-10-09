#include <memory>
#include "common/macros.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule

  std::vector<AbstractPlanNodeRef> childrens;
  for (const auto &child : plan->GetChildren()) {
    childrens.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimiza_plan = plan->CloneWithChildren(std::move(childrens));

  if (optimiza_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimiza_plan);
    const auto &limit = limit_plan.GetLimit();

    BUSTUB_ENSURE(limit_plan.children_.size() == 1, "Limit Plan should have exactly 1 child.");
    if (optimiza_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*optimiza_plan->GetChildAt(0));
      const auto &order_bys = sort_plan.GetOrderBy();

      BUSTUB_ENSURE(sort_plan.children_.size() == 1, "Sort Plan should have exactly 1 child.");

      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildAt(0), order_bys, limit);
    }
  }
  return optimiza_plan;
}

}  // namespace bustub
