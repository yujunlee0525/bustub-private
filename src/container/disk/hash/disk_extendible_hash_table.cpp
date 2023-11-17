//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  page_id_t page_id;
  auto page = bpm_->NewPageGuarded(&page_id).UpgradeWrite();
  header_page_id_ = page_id;
  auto header = page.AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth_);
  page.Drop();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto header_id = GetHeaderPageId();
  if (header_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard page = bpm_->FetchPageRead(header_id);
  auto header = page.As<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    page.Drop();
    return false;
  }
  ReadPageGuard d_page = bpm_->FetchPageRead(directory_page_id);
  page.Drop();
  auto directory = d_page.As<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    d_page.Drop();
    return false;
  }
  ReadPageGuard b_page = bpm_->FetchPageRead(bucket_page_id);
  d_page.Drop();
  auto bucket = b_page.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V res;
  auto found = bucket->Lookup(key, res, cmp_);
  if (found) {
    (*result).push_back(res);
  }
  b_page.Drop();
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto header_id = GetHeaderPageId();
  if (header_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard page = bpm_->FetchPageWrite(header_id);
  auto header = page.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);

  if (directory_page_id == INVALID_PAGE_ID) {
    if (!InsertToNewDirectory(header, directory_idx, hash, key, value)) {
      page.Drop();
      return false;
    }
    directory_page_id = header->GetDirectoryPageId(directory_idx);
  }

  WritePageGuard d_page = bpm_->FetchPageWrite(directory_page_id);
  page.Drop();
  auto directory = d_page.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    auto inserted = InsertToNewBucket(directory, bucket_idx, key, value);
    d_page.Drop();
    return inserted;
  }
  WritePageGuard b_page = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = b_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket->IsFull()) {
    b_page.Drop();
    auto inserted = SplitInsert(directory, bucket_idx, hash, key, value);
    d_page.Drop();
    return inserted;
  }
  d_page.Drop();
  auto inserted = bucket->Insert(key, value, cmp_);
  b_page.Drop();
  return inserted;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitInsert(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                    uint32_t hash, const K &key, const V &value) -> bool {
  if (directory->GetLocalDepth(bucket_idx) == directory->GetMaxDepth()) {
    // directory cant grow
    return false;
  }
  if (directory->GetLocalDepth(bucket_idx) == directory->GetGlobalDepth()) {
    directory->IncrGlobalDepth();
  }
  directory->IncrLocalDepth(bucket_idx);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  WritePageGuard b_page = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = b_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // Create split bucket
  auto split_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
  directory->SetLocalDepth(split_bucket_idx, directory->GetLocalDepth(bucket_idx));
  page_id_t page_id;
  WritePageGuard split_b_page = bpm_->NewPageGuarded(&page_id).UpgradeWrite();
  if (page_id == INVALID_PAGE_ID) {
    split_b_page.Drop();
    b_page.Drop();
    return false;
  }
  directory->SetBucketPageId(split_bucket_idx, page_id);
  // allocate array elements on new split bucket and remove from original bucket
  auto split_bucket = split_b_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  split_bucket->Init(bucket_max_size_);
  size_t size = bucket->Size();
  int i = size - 1;
  while (i >= 0) {
    auto k = bucket->KeyAt(i);
    auto v = bucket->ValueAt(i);
    auto index = directory->HashToBucketIndex(Hash(k));
    if (index == split_bucket_idx) {
      if (split_bucket->Insert(k, v, cmp_)) {
        bucket->Remove(k, cmp_);
      }
    }
    i -= 1;
  }
  split_b_page.Drop();
  b_page.Drop();

  // Change other bucket_idx to the split bucket page
  auto diff = 1 << directory->GetLocalDepth(split_bucket_idx);
  auto min_mask = diff - 1;
  for (size_t i = min_mask & split_bucket_idx; i < directory->Size(); i += diff) {
    directory->SetLocalDepth(i, directory->GetLocalDepth(split_bucket_idx));
    directory->SetBucketPageId(i, directory->GetBucketPageId(split_bucket_idx));
  }
  // change other bucket_idx to the original page
  for (size_t i = min_mask & bucket_idx; i < directory->Size(); i += diff) {
    directory->SetLocalDepth(i, directory->GetLocalDepth(bucket_idx));
    directory->SetBucketPageId(i, directory->GetBucketPageId(bucket_idx));
  }
  // new bucketidx in case directory grew (should be split or original)
  bucket_idx = directory->HashToBucketIndex(hash);

  WritePageGuard guard = bpm_->FetchPageWrite(directory->GetBucketPageId(bucket_idx));
  bucket = guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // if split still full recrusively split and insert
  if (bucket->IsFull()) {
    guard.Drop();
    return SplitInsert(directory, bucket_idx, hash, key, value);
  }
  guard.Drop();
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t new_page_id;
  auto new_page = bpm_->NewPageGuarded(&new_page_id).UpgradeWrite();
  if (new_page_id == INVALID_PAGE_ID) {
    new_page.Drop();
    return false;
  }
  auto dir = new_page.AsMut<ExtendibleHTableDirectoryPage>();
  dir->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, new_page_id);
  new_page.Drop();
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_page_id;
  auto new_page = bpm_->NewPageGuarded(&new_page_id).UpgradeWrite();
  if (new_page_id == INVALID_PAGE_ID) {
    new_page.Drop();
    return false;
  }
  auto bucket = new_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);
  auto inserted = bucket->Insert(key, value, cmp_);
  directory->SetBucketPageId(bucket_idx, new_page_id);
  new_page.Drop();
  return inserted;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto header_id = GetHeaderPageId();
  if (header_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard page = bpm_->FetchPageWrite(header_id);
  auto header = page.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    page.Drop();
    return false;
  }
  WritePageGuard d_page = bpm_->FetchPageWrite(directory_page_id);
  page.Drop();
  auto directory = d_page.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    d_page.Drop();
    return false;
  }
  WritePageGuard b_page = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = b_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto removed = bucket->Remove(key, cmp_);
  if (!bucket->IsEmpty()) {
    d_page.Drop();
    b_page.Drop();
    return removed;
  }
  // bucket now empty => merge
  b_page.Drop();
  Merge(directory, bucket_idx, hash);
  d_page.Drop();
  return removed;
}
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::Merge(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                              uint32_t hash) {
  auto local_depth = directory->GetLocalDepth(bucket_idx);
  if (local_depth == 0) {
    return;
  }
  auto split_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
  auto split_local_depth = directory->GetLocalDepth(split_bucket_idx);
  auto split_bucket_page_id = directory->GetBucketPageId(split_bucket_idx);
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (split_local_depth != local_depth) {
    return;
  }
  // same local depth => can merge
  directory->SetBucketPageId(bucket_idx, split_bucket_page_id);
  directory->DecrLocalDepth(bucket_idx);
  directory->DecrLocalDepth(split_bucket_idx);
  // set others mapped to the same bucket to the new merged bucket
  for (size_t i = 0; i < directory->Size(); i++) {
    if (directory->GetBucketPageId(i) == split_bucket_page_id || directory->GetBucketPageId(i) == bucket_page_id) {
      directory->SetBucketPageId(i, split_bucket_page_id);
      directory->SetLocalDepth(i, directory->GetLocalDepth(bucket_idx));
    }
  }
  // shrink directory
  while (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }

  // get new bucket_idx in case the table has shrunk
  bucket_idx = directory->HashToBucketIndex(hash);
  // if local depth is 0 there is no need to merge
  if (directory->GetLocalDepth(bucket_idx) == 0) {
    return;
  }
  split_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
  WritePageGuard guard = bpm_->FetchPageWrite(directory->GetBucketPageId(bucket_idx));
  WritePageGuard split_guard = bpm_->FetchPageWrite(directory->GetBucketPageId(split_bucket_idx));
  auto merged_bucket = guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto merged_split_bucket = split_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (merged_bucket->IsEmpty()) {
    // recrursive merge the bucket
    guard.Drop();
    split_guard.Drop();
    Merge(directory, bucket_idx, hash);
  } else if (merged_split_bucket->IsEmpty()) {
    // recrusive merge the split bucket with the bucket
    guard.Drop();
    split_guard.Drop();
    Merge(directory, split_bucket_idx, hash);
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
