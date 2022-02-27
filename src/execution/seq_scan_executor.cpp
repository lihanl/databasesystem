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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  predicate_ = plan_->GetPredicate();
  txn_ = exec_ctx_->GetTransaction();
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->GetTableOid());
  iterator_ = TableIterator(table_info_->table_->Begin(txn_));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iterator_ != table_info_->table_->End()) {
    Tuple cur_tuple = *iterator_;
    uint32_t col_count = GetOutputSchema()->GetColumnCount();
    auto cols = GetOutputSchema()->GetColumns();
    std::vector<Value> tuple_values;
    tuple_values.reserve(col_count);
    try {
      for (auto &col : cols) {
        uint32_t col_idx = table_info_->schema_.GetColIdx(col.GetName());
        tuple_values.push_back(cur_tuple.GetValue(&table_info_->schema_, col_idx));
      }
    } catch (std::logic_error &error) {
      for (uint32_t col_idx = 0; col_idx < col_count; col_idx++) {
        tuple_values.push_back(cur_tuple.GetValue(&table_info_->schema_, col_idx));
      }
    }

    *tuple = Tuple(tuple_values, GetOutputSchema());
    *rid = cur_tuple.GetRid();
    iterator_++;
    if (predicate_ == nullptr || predicate_->Evaluate(&cur_tuple, &table_info_->schema_).GetAs<bool>()) {
      return true;
    }
  }
  return false;
}
}  // namespace bustub
