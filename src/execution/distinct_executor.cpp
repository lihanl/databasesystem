//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  child_executor_->Init();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple tmp_tuple;
  const auto *output_schema = plan_->OutputSchema();
  while (child_executor_->Next(&tmp_tuple, rid)) {
    DistinctValueKey dk;
    for (size_t i = 0; i < output_schema->GetColumnCount(); i++) {
      dk.values_.push_back(tmp_tuple.GetValue(GetOutputSchema(), i));
    }
    if (hm_.count(dk) == 0) {
      *tuple = Tuple(dk.values_, GetOutputSchema());
      hm_.insert({dk, 1});
      return true;
    }
  }
  return false;
}

}  // namespace bustub
