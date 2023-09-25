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
#include "catalog/column.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() { iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ != table_info_->table_->End()) {
    *tuple = *iter_;
    *rid = tuple->GetRid();
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
