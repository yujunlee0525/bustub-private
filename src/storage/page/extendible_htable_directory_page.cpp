//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (auto &iter : local_depths_) {
    iter = 0;
  }
  for (auto &iter : bucket_page_ids_) {
    iter = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  auto mask = (1 << global_depth_) - 1;
  return hash & mask;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= HTABLE_DIRECTORY_ARRAY_SIZE) {
    return INVALID_PAGE_ID;
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE) {
    bucket_page_ids_[bucket_idx] = bucket_page_id;
  }
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  auto offset = 1 << (local_depths_[bucket_idx] - 1);
  return bucket_idx ^ offset;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  assert(global_depth_ < max_depth_);
  auto size = Size();
  for (size_t i = 0, j = size; i < size; i++, j++) {
    bucket_page_ids_[j] = bucket_page_ids_[i];
    local_depths_[j] = local_depths_[i];
  }
  global_depth_ += 1;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    return;
  }
  global_depth_ -= 1;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;
  }
  for (size_t i = 0; i < Size(); i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE) {
    local_depths_[bucket_idx] = local_depth;
  }
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE) {
    if (local_depths_[bucket_idx] < max_depth_) {
      local_depths_[bucket_idx] += 1;
    }
  }
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE) {
    if (local_depths_[bucket_idx] > 0) {
      local_depths_[bucket_idx] -= 1;
    }
  }
}

}  // namespace bustub
