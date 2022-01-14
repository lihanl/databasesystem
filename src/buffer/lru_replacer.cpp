//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <iostream>
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { capacity_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();
  if (dlink_.empty()) {
    // std::cout << "dlink empty!"<<"\n";
    latch_.unlock();
    return false;
  }
  *frame_id = dlink_.back();
  unpinned_map_.erase(*frame_id);
  dlink_.pop_back();
  latch_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  if (unpinned_map_.count(frame_id) == 0) {
    latch_.unlock();
    return;
  }
  auto cur = unpinned_map_[frame_id];
  dlink_.erase(cur);
  unpinned_map_.erase(frame_id);
  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  if (unpinned_map_.count(frame_id) != 0 || unpinned_map_.size() == capacity_) {
    latch_.unlock();
    return;
  }
  dlink_.push_front(frame_id);
  unpinned_map_.insert(make_pair(frame_id, dlink_.begin()));
  latch_.unlock();
}

size_t LRUReplacer::Size() { return unpinned_map_.size(); }

}  // namespace bustub
