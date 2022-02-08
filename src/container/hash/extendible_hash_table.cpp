//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  page_id_t first_bucket_page_id;
  HashTableDirectoryPage *d_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
  d_page->InitTable();
  d_page->IncrGlobalDepth();
  buffer_pool_manager_->NewPage(&first_bucket_page_id);
  d_page->SetBucketPageId(0, first_bucket_page_id);
  d_page->SetBucketPageId(1, first_bucket_page_id);
  d_page->SetLocalDepth(0, 0);
  d_page->SetLocalDepth(1, 0);

  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(first_bucket_page_id, false, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t mask = dir_page->GetGlobalDepthMask();
  uint32_t hash_key = Hash(key);
  return hash_key & mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t d_index = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(d_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *d_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
  return d_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  HASH_TABLE_BUCKET_TYPE *bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t index = KeyToDirectoryIndex(key, directory_page);

  page_id_t bucket_page_id = directory_page->GetBucketPageId(index);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(bucket_page)->RLatch();
  bool ret = bucket_page->GetValue(key, comparator_, result);

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t index = KeyToDirectoryIndex(key, directory_page);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(index);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  if (bucket_page->Insert(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    table_latch_.WUnlock();
    return true;
  }
  if (bucket_page->IsFull() && !bucket_page->CheckKeyValueExist(key, value, comparator_)) {
    // full and does not have the kv pair, try split insert
    bool ret = SplitInsert(transaction, key, value);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    table_latch_.WUnlock();
    return ret;
  }
  // existed same kv pair, return false
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();
  table_latch_.WUnlock();
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t index = KeyToDirectoryIndex(key, directory_page);
  page_id_t old_page_id = directory_page->GetBucketPageId(index);
  HASH_TABLE_BUCKET_TYPE *old_page = FetchBucketPage(old_page_id);
  uint32_t local_depth = directory_page->GetLocalDepth(index);

  uint32_t new_idx = index;
  page_id_t new_page_id = INVALID_PAGE_ID;
  if (local_depth < directory_page->GetGlobalDepth()) {
    // no need for increasing global depth
    directory_page->IncrLocalDepth(index);
    uint32_t local_depth = directory_page->GetLocalDepth(index);
    new_idx = index ^ (1 << (local_depth - 1));
    // get new index ***1*** , index is ***0***, new index ***0***, index is ***1**
  } else if (local_depth == directory_page->GetGlobalDepth()) {
    directory_page->IncrLocalDepth(
        index);  // increase local depth and the global depth. two bucket point to same page now
    uint32_t global_depth = directory_page->GetGlobalDepth();
    new_idx = index | (1 << (global_depth - 1));  // index 0****, new_idx 1****.
    if (new_idx >= DIRECTORY_ARRAY_SIZE) {        // if out of bound
      directory_page->DecrLocalDepth(index);      // decrease local and global depth
      buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
      buffer_pool_manager_->UnpinPage(old_page_id, false, nullptr);
      return false;
    }
  }
  HASH_TABLE_BUCKET_TYPE *new_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_page_id));
  // redirect the bucket page ids, half of them point to new page id
  reinterpret_cast<Page *>(new_page)->WLatch();
  directory_page->SeperatePageId(index, new_idx, new_page_id);
  std::vector<KeyType> keys;
  std::vector<ValueType> values;
  old_page->EmptyArray(&keys, &values);
  keys.push_back(key);
  values.push_back(value);
  for (size_t i = 0; i < keys.size(); i++) {
    uint32_t tmp_index = KeyToDirectoryIndex(keys[i], directory_page);
    page_id_t tmp_page_id = directory_page->GetBucketPageId(tmp_index);
    if (tmp_page_id == new_page_id) {
      new_page->Insert(keys[i], values[i], comparator_);
    } else {
      old_page->Insert(keys[i], values[i], comparator_);
    }
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(old_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(new_page_id, true, nullptr);
  reinterpret_cast<Page *>(new_page)->WUnlatch();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t index = KeyToDirectoryIndex(key, directory_page);
  page_id_t page_id = directory_page->GetBucketPageId(index);
  HASH_TABLE_BUCKET_TYPE *cur_page = FetchBucketPage(page_id);
  reinterpret_cast<Page *>(cur_page)->WLatch();
  if (!cur_page->Remove(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
    reinterpret_cast<Page *>(cur_page)->WUnlatch();
    table_latch_.WUnlock();
    return false;
  }

  if (cur_page->IsEmpty() && directory_page->GetLocalDepth(index) > 0) {
    reinterpret_cast<Page *>(cur_page)->WUnlatch();
    Merge(transaction, key, value);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
  } else {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, true, nullptr);
    reinterpret_cast<Page *>(cur_page)->WUnlatch();
  }
  table_latch_.WUnlock();
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t index = KeyToDirectoryIndex(key, directory_page);
  // std::cout<< "Before Merge\n";
  // directory_page->PrintDirectory();

  uint32_t local_depth = directory_page->GetLocalDepth(index);
  uint32_t merge_page_index = index ^ (1 << (local_depth - 1));
  if (local_depth == 0 || local_depth != directory_page->GetLocalDepth(merge_page_index)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    return;
  }

  page_id_t page_id = directory_page->GetBucketPageId(index);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page_id);
  reinterpret_cast<Page *>(bucket_page)->RLatch();
  if (!bucket_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
    reinterpret_cast<Page *>(bucket_page)->RUnlatch();
    return;
  }
  buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  if (directory_page->GetLocalDepth(index) == directory_page->GetGlobalDepth()) {
    page_id_t merge_page_id = directory_page->GetBucketPageId(merge_page_index);
    directory_page->SetBucketPageId(index, merge_page_id);
    directory_page->SetBucketPageId(merge_page_index, merge_page_id);
    directory_page->DecrLocalDepth(index);
  } else {
    uint32_t next_local_mask =
        ((1 << local_depth) - 1) >> 1;  // create mask for local depth (after decreased, thats what the >>1 for)
    page_id_t merge_page_id = directory_page->GetBucketPageId(merge_page_index);
    directory_page->MergePageId(index, next_local_mask, merge_page_id);
    directory_page->DecrLocalDepth(index);
  }
  uint32_t new_index = KeyToDirectoryIndex(key, directory_page);
  page_id_t new_page_id = directory_page->GetBucketPageId(new_index);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);

  HASH_TABLE_BUCKET_TYPE *new_page = FetchBucketPage(new_page_id);
  reinterpret_cast<Page *>(new_page)->WLatch();
  if (new_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(new_page_id, true, nullptr);
    reinterpret_cast<Page *>(new_page)->WUnlatch();
    Merge(transaction, key, value);
  } else {
    buffer_pool_manager_->UnpinPage(new_page_id, false, nullptr);
    reinterpret_cast<Page *>(new_page)->WUnlatch();
  }
  // std::cout<< "After Merge\n";
  // directory_page->PrintDirectory();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
