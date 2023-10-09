//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <utility>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID emit_rid{};
  // require left tuple
  while (right_tuple_idx_ >= 0 || left_executor_->Next(&left_tuple_, &emit_rid)) {
    std::vector<Value> values{};
    for (uint32_t i = right_tuple_idx_ < 0 ? 0 : right_tuple_idx_; i < right_tuples_.size(); i++) {
      // search matched tuple
      if (Match(&left_tuple_, &right_tuples_[i])) {
        for (uint32_t left_index = 0; left_index < left_executor_->GetOutputSchema().GetColumnCount(); left_index++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), left_index));
        }
        for (uint32_t right_index = 0; right_index < right_executor_->GetOutputSchema().GetColumnCount();
             right_index++) {
          values.push_back(right_tuples_[i].GetValue(&right_executor_->GetOutputSchema(), right_index));
        }

        *tuple = Tuple(values, &GetOutputSchema());
        right_tuple_idx_ = i + 1;
        return true;
      }
    }

    if (right_tuple_idx_ == -1 && plan_->GetJoinType() == JoinType::LEFT) {
      for (uint32_t left_index = 0; left_index < left_executor_->GetOutputSchema().GetColumnCount(); left_index++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), left_index));
      }

      for (uint32_t right_index = 0; right_index < right_executor_->GetOutputSchema().GetColumnCount(); right_index++) {
        values.push_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(right_index).GetType()));
      }

      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
    right_tuple_idx_ = -1;
  }
  return false;
}

auto NestedLoopJoinExecutor::Match(bustub::Tuple *left_tuple, bustub::Tuple *right_tuple) const -> bool {
  auto value = plan_->Predicate().EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                                               right_executor_->GetOutputSchema());
  return !value.IsNull() && value.GetAs<bool>();
}

}  // namespace bustub
