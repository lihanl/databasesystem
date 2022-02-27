//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/update_executor.h"
#include <iostream>
#include <memory>

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  txn_ = exec_ctx_->GetTransaction();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  RID new_rid;
  Tuple old_tuple;
  Tuple new_tuple;
  if (!child_executor_->Next(&old_tuple, &new_rid)) {
    return false;
  }
  new_tuple = GenerateUpdatedTuple(old_tuple);
  if (!table_info_->table_->UpdateTuple(new_tuple, new_rid, txn_)) {
    return false;
  }

  std::vector<IndexInfo *> indexes = catalog_->GetTableIndexes(table_info_->name_);
  for (auto & index : indexes) {
    const auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(),
                                                index->index_->GetKeyAttrs());
    const auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(),
                                                index->index_->GetKeyAttrs());
    index->index_->DeleteEntry(old_key, new_rid, txn_);
    index->index_->InsertEntry(new_key, new_rid, txn_);
  }
  return true;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
