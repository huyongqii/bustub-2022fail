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
#include "common/rid.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      index_info_{exec_ctx->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      iter_(tree_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  tree_->ScanKey(Tuple(), &rids_, exec_ctx_->GetTransaction());
  rids_iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (rids_iter_ != rids_.end()) {
    *rid = *rids_iter_;
    auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    rids_iter_++;
    return result;
  }
  return false;
}
}  // namespace bustub
