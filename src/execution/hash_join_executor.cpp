//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  txn_ = exec_ctx_->GetTransaction();
  left_key_expression_ = plan_->LeftJoinKeyExpression();
  right_key_expression_ = plan_->RightJoinKeyExpression();
  left_child_->Init();
  right_child_->Init();

  more_combination_ = false;
  cur_id_ = -1;
  Tuple tuple;
  RID rid;
  /** build my hashmap**/
  while (left_child_->Next(&tuple, &rid)) {
    HashJoinKey key;
    key.join_keys_.push_back(left_key_expression_->Evaluate(&tuple, left_child_->GetOutputSchema()));
    InsertCombine(key, tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple right_tuple;
  HashJoinKey right_key;
  if (more_combination_) {
    std::vector<Value> vals = CombinedTuples(cur_vector_[cur_id_++], cur_right_tuple_);
    *tuple = Tuple(vals, GetOutputSchema());
    more_combination_ = cur_id_ < cur_vector_.size();
    return true;
  }
  while (right_child_->Next(&right_tuple, rid)) {
    right_key.join_keys_.push_back(right_key_expression_->Evaluate(&right_tuple, left_child_->GetOutputSchema()));
    if (hm_.count(right_key) == 0) {
      continue;
    }
    more_combination_ = hm_[right_key].size() > 1;
    cur_id_ = 0;
    cur_vector_ = hm_[right_key];
    cur_right_tuple_ = right_tuple;
    std::vector<Value> vals = CombinedTuples(hm_[right_key][cur_id_++], right_tuple);
    *tuple = Tuple(vals, GetOutputSchema());
    return true;
  }
  return false;
}

std::vector<Value> HashJoinExecutor::CombinedTuples(const Tuple &left_tuple, const Tuple &right_tuple) {
  std::vector<Value> value;
  for (const auto &col : GetOutputSchema()->GetColumns()) {
    value.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_child_->GetOutputSchema(), &right_tuple,
                                                right_child_->GetOutputSchema()));
  }
  return value;
}

void HashJoinExecutor::InsertCombine(const HashJoinKey &join_key, const Tuple &join_tuple) {
  if (hm_.count(join_key) == 0) {
    std::vector<Tuple> new_vector;
    hm_.insert({join_key, new_vector});
  }
  hm_[join_key].push_back(join_tuple);
}

}  // namespace bustub
