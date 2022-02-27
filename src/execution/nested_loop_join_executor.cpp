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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  txn_ = exec_ctx_->GetTransaction();
  left_executor_->Init();
  right_executor_->Init();
  predicate_ = plan_->Predicate();
  cur_left_available_ = false;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  const auto *output_schema = plan_->OutputSchema();
  while (cur_left_available_ || left_executor_->Next(&left_tuple_, rid)) {
    cur_left_available_ = true;
    while (right_executor_->Next(&right_tuple, rid)) {
      if (predicate_ != nullptr && !predicate_
                                        ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                       right_executor_->GetOutputSchema())
                                        .GetAs<bool>()) {
        continue;
      }
      std::vector<Value> tmp_values;
      auto cols = output_schema->GetColumns();
      tmp_values.reserve(output_schema->GetColumnCount());
      for (auto &col : cols) {
        tmp_values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                         right_executor_->GetOutputSchema()));
      }
      *tuple = Tuple(tmp_values, output_schema);
      *rid = left_tuple_.GetRid();
      return true;
    }
    cur_left_available_ = false;
    right_executor_->Init();
  }
  return false;
}
}  // namespace bustub
