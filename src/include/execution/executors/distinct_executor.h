//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
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
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

struct DistinctValueKey {
  std::vector<Value> values_;

  bool operator==(const DistinctValueKey &other) const {
    for (uint32_t i = 0; i < other.values_.size(); i++) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

class DistinctHashFunction {
 public:
  std::size_t operator()(const DistinctValueKey &distinct_key) const {
    size_t map_key = 0;
    for (const auto &key : distinct_key.values_) {
      if (!key.IsNull()) {
        map_key = bustub::HashUtil::CombineHashes(map_key, bustub::HashUtil::HashValue(&key));
      }
    }
    return map_key;
  }

  // private:
  //   HashFunction<Value> hash_fn_;
};

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  void Insert(const DistinctValueKey &distinct_key);
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  Catalog *catalog_;
  Transaction *txn_;
  std::unordered_map<DistinctValueKey, size_t, DistinctHashFunction> hm_{};
};
}  // namespace bustub
