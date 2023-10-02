//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <mutex>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  this->latch_.lock();
  size_t max_k = this->current_timestamp_ + 1;
  for (auto &iter : this->node_store_) {
    auto node = iter.second;
    if (!node.is_evictable_) {
      continue;
    }
    if (node.k_ >= this->k_) {
      max_k = std::min<size_t>(max_k, node.history_.front());
      if (max_k == node.history_.front()) {
        *frame_id = iter.first;
      }
    } else {
      max_k = this->current_timestamp_ + 1;
      break;
    }
  }
  if (max_k == this->current_timestamp_ + 1) {
    for (auto &iter : this->node_store_) {
      auto node = iter.second;
      if (!node.is_evictable_ || node.k_ >= this->k_) {
        continue;
      }
      max_k = std::min<size_t>(max_k, node.history_.front());
      if (max_k == node.history_.front()) {
        *frame_id = iter.first;
      }
    }
  }
  this->latch_.unlock();
  this->Remove(*frame_id);
  return (max_k != this->current_timestamp_ + 1);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  size_t frame = frame_id;
  BUSTUB_ASSERT(frame <= this->replacer_size_, "frame is larger than replacer size");
  std::scoped_lock lock(this->latch_);
  auto node = this->node_store_.find(frame_id);
  if (node != this->node_store_.end()) {
    node->second.history_.push_back(this->current_timestamp_);
    node->second.k_ += 1;
    if (node->second.k_ > this->k_) {
      node->second.k_ -= 1;
      node->second.history_.pop_front();
    }
  } else {
    std::list<size_t> li{this->current_timestamp_};
    auto node = LRUKNode(1, li);
    this->node_store_.insert({frame_id, node});
  }
  this->current_timestamp_ += 1;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(this->latch_);
  auto node = this->node_store_.find(frame_id);
  if (node == this->node_store_.end()) {
    return;
  }
  auto prev_evictable = node->second.is_evictable_;
  node->second.is_evictable_ = set_evictable;
  if (set_evictable && !prev_evictable) {
    this->curr_size_ += 1;
  } else if (!set_evictable && prev_evictable) {
    this->curr_size_ -= 1;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(this->latch_);
  auto node = this->node_store_.find(frame_id);
  if (node == this->node_store_.end()) {
    return;
  }
  BUSTUB_ASSERT(node->second.is_evictable_, "frame is not evictable");
  this->node_store_.erase(frame_id);
  this->curr_size_ -= 1;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(this->latch_);
  auto size = this->curr_size_;
  return size;
}

}  // namespace bustub
