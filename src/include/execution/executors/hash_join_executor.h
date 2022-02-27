//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include <unordered_map>
#include <vector>
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HashJoinKey {
  /** join keys **/
  std::vector<Value> join_keys_;

  bool operator==(const HashJoinKey &other) const {
    for (uint32_t i = 0; i < other.join_keys_.size(); i++) {
      if (join_keys_[i].CompareEquals(other.join_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

class HashJoinKeysHashFunction {
 public:
  std::size_t operator()(const HashJoinKey &hash_join_key) const {
    size_t map_key = 0;
    for (const auto &key : hash_join_key.join_keys_) {
      if (!key.IsNull()) {
        map_key = bustub::HashUtil::CombineHashes(map_key, bustub::HashUtil::HashValue(&key));
      }
    }
    return map_key;
  }
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  std::vector<Value> CombinedTuples(const Tuple &left_tuple, const Tuple &right_tuple);
  void InsertCombine(const HashJoinKey &join_key, const Tuple &join_tuple);

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  const AbstractExpression *left_key_expression_;
  const AbstractExpression *right_key_expression_;
  Transaction *txn_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::unordered_map<HashJoinKey, std::vector<Tuple>, HashJoinKeysHashFunction> hm_{};
  bool more_combination_;
  std::vector<Tuple> cur_vector_;
  size_t cur_id_;
  Tuple cur_right_tuple_;
};

}  // namespace bustub
