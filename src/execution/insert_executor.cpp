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

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  is_raw_ = plan_->IsRawInsert();
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  txn_ = exec_ctx_->GetTransaction();
  if (!is_raw_) {
    child_executor_->Init();
  }
  if (is_raw_) {
    raw_values_ = plan_->RawValues();
    raw_value_iter_ = raw_values_.begin();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Tuple new_tuple;
  RID new_rid;
  if (is_raw_) {
    if (raw_value_iter_ == raw_values_.end()) {
      return false;
    }
    *tuple = Tuple(*raw_value_iter_, &table_info_->schema_);
    if (!table_info_->table_->InsertTuple(*tuple, &new_rid, txn_)) {
      return false;
    }
    raw_value_iter_++;
  } else {
    if (!child_executor_->Next(tuple, &new_rid)) {
      return false;
    }
    if (!table_info_->table_->InsertTuple(*tuple, &new_rid, txn_)) {
      return false;
    }
  }
  std::vector<IndexInfo *> indexes = catalog_->GetTableIndexes(table_info_->name_);
  for (auto & index: indexes) {
    auto key = tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(),
                                   index->index_->GetKeyAttrs());
    index->index_->InsertEntry(key, new_rid, txn_);
  }
  return true;
}
}  // namespace bustub
