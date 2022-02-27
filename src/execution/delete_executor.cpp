//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  txn_ = exec_ctx_->GetTransaction();
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  RID new_rid;
  Tuple delete_tuple;
  TableHeap *table_heap = table_info_->table_.get();
  if (!child_executor_->Next(&delete_tuple, &new_rid)) {
    return false;
  }
  if (!table_heap->MarkDelete(new_rid, txn_)) {
    return false;
  }
  std::vector<IndexInfo *> indexes = catalog_->GetTableIndexes(table_info_->name_);
  for (auto & index : indexes) {
    auto key = delete_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(),
                                         index->index_->GetKeyAttrs());
    index->index_->DeleteEntry(key, new_rid, txn_);
  }
  return true;
}
}  // namespace bustub
