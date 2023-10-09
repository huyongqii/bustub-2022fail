//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <cstdint>
#include "binder/table_ref/bound_join_ref.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child_executor)),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->index_oid_)),
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple{};
  RID emit_rid{};
  std::vector<Value> values;
  while (child_->Next(&left_tuple, &emit_rid)) {
    // Get the key of left_tuple through the key predicate
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_->GetOutputSchema());
    std::vector<RID> rids;
    tree_->ScanKey(Tuple{{value}, index_info_->index_->GetKeySchema()}, &rids, exec_ctx_->GetTransaction());

    Tuple right_tuple;
    if (!rids.empty()) {
      // require first matched right tuple
      table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());
      for (uint32_t left_idx = 0; left_idx < child_->GetOutputSchema().GetColumnCount(); left_idx++) {
        values.push_back(left_tuple.GetValue(&child_->GetOutputSchema(), left_idx));
      }

      for (uint32_t right_idx = 0; right_idx < plan_->InnerTableSchema().GetColumnCount(); right_idx++) {
        values.push_back(right_tuple.GetValue(&plan_->InnerTableSchema(), right_idx));
      }

      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    // right_tuple is null
    if (plan_->GetJoinType() == JoinType::LEFT) {
      table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());
      for (uint32_t left_idx = 0; left_idx < child_->GetOutputSchema().GetColumnCount(); left_idx++) {
        values.push_back(left_tuple.GetValue(&child_->GetOutputSchema(), left_idx));
      }
      // build a null type cloumn
      for (uint32_t right_idx = 0; right_idx < plan_->InnerTableSchema().GetColumnCount(); right_idx++) {
        values.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(right_idx).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
  }
  return false;
}

}  // namespace bustub
