//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  txn_ = exec_ctx_->GetTransaction();
  having_ = plan_->GetHaving();
  child_->Init();
  aht_.GenerateInitialAggregateValue();

  Tuple tmp_tuple;
  while (child_->Next(&tmp_tuple, &this_rid_)) {
    AggregateKey tmp_key;
    AggregateValue tmp_value;
    tmp_key = MakeAggregateKey(&tmp_tuple);
    tmp_value = MakeAggregateValue(&tmp_tuple);
    aht_.InsertCombine(tmp_key, tmp_value);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    const std::vector<Value> group_by = aht_iterator_.Key().group_bys_;
    const std::vector<Value> aggregate = aht_iterator_.Val().aggregates_;
    ++aht_iterator_;
    if (having_ == nullptr || having_->EvaluateAggregate(group_by, aggregate).GetAs<bool>()) {
      auto cols = GetOutputSchema()->GetColumns();
      std::vector<Value> tmp_vals;
      tmp_vals.reserve(cols.size());
      for (auto &col : cols) {
        tmp_vals.push_back(col.GetExpr()->EvaluateAggregate(group_by, aggregate));
      }
      *tuple = Tuple(tmp_vals, GetOutputSchema());
      *rid = this_rid_;
      return true;
    }
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
