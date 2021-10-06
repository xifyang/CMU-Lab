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

namespace bustub {

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (lru_list_.empty() || lru_map_.empty()) {
    return false;
  }
  frame_id_t victim_fid = lru_list_.back();
  lru_list_.pop_back();
  lru_map_.erase(victim_fid);
  *frame_id = victim_fid;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (lru_map_.find(frame_id) == lru_map_.end()) {
    // frame_id don't exit
    return;
  }
  auto pin_iterator = lru_map_[frame_id];
  lru_list_.splice(lru_list_.begin(), lru_list_, pin_iterator);
  lru_list_.pop_front();
  lru_map_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (lru_map_.find(frame_id) != lru_map_.end() || lru_map_.size() == capacity_) {
    // frame_id exit or list is full
    return;
  }
  lru_list_.push_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
}

}  // namespace bustub
