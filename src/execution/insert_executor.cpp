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
#include <utility>
#include <vector>

#include "catalog/column.h"
#include "execution/executors/insert_executor.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), chirld_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void InsertExecutor::Init() {
  chirld_executor_->Init();
  table_indexs_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int count = 0;
  while (chirld_executor_->Next(tuple, rid)) {
    if (table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
      for (auto index : table_indexs_) {
        auto key = (*tuple).KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      }
    }
    count++;
  }
  std::vector<Value> value{};
  value.reserve(GetOutputSchema().GetColumnCount());
  value.emplace_back(INTEGER, count);
  *tuple = Tuple(value, &GetOutputSchema());
  is_end_ = true;
  return true;
}
}  // namespace bustub
