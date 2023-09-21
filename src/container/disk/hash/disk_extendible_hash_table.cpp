//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/types.h>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "common/rwlatch.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "nodes/nodes.hpp"
#include "storage/page/hash_table_directory_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                         const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  directory_page_id_ = INVALID_PAGE_ID;
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
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> page_id_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  HashTableDirectoryPage *dir_page;
  if (directory_page_id_ == INVALID_PAGE_ID) {
    page_id_t dir_page_id;
    Page *page = buffer_pool_manager_->NewPage(&dir_page_id);
    assert(page != nullptr);
    dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
    directory_page_id_ = dir_page_id;
    dir_page->SetPageId(directory_page_id_);

    page_id_t bucket_page_id;
    page = buffer_pool_manager_->NewPage(&bucket_page_id);
    assert(page != nullptr);
    dir_page->SetBucketPageId(0, bucket_page_id);

    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  }

  assert(directory_page_id_ != INVALID_PAGE_ID);
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page != nullptr);
  dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);

  // 读取数据
  bool ret = bucket->GetValue(key, comparator_, result);

  // 记得Unpin
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  if (!bucket->IsFull()) {
    bool ret = bucket->Insert(key, value, comparator_);
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    return ret;
  }

  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();

  // get split_bucket info
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  int64_t split_bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_bucket_depth = dir_page->GetLocalDepth(split_bucket_index);

  if (split_bucket_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  dir_page->IncrLocalDepth(split_bucket_index);

  page_id_t split_bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *split_bucket = FetchBucketPage(split_bucket_page_id);
  uint32_t origin_array_size = split_bucket->NumReadable();
  MappingType *origin_array = split_bucket->GetArrayCopy();
  split_bucket->Reset();

  // init image bucket
  page_id_t image_bucket_page_id;
  Page *image_bucket_page = buffer_pool_manager_->NewPage(&image_bucket_page_id);
  assert(image_bucket_page != nullptr);

  image_bucket_page->WLatch();

  HASH_TABLE_BUCKET_TYPE *image_bucket = FetchBucketPage(image_bucket_page_id);
  uint32_t split_image_bucket_index = dir_page->GetSplitImageIndex(split_bucket_index);
  dir_page->SetLocalDepth(split_image_bucket_index, dir_page->GetLocalDepth(split_bucket_index));
  dir_page->SetBucketPageId(split_image_bucket_index, image_bucket_page_id);

  for (uint32_t i = 0; i < origin_array_size; i++) {
    uint32_t target_bucket_index = Hash(origin_array[i].first) & dir_page->GetLocalDepthMask(split_bucket_index);
    page_id_t target_bucket_index_page = dir_page->GetBucketPageId(target_bucket_index);
    assert((target_bucket_index_page == split_bucket_page_id) || (target_bucket_index_page == image_bucket_page_id));
    if (target_bucket_index_page == split_bucket_page_id) {
      assert(split_bucket->Insert(origin_array[i].first, origin_array[i].second, comparator_));
    } else {
      assert(image_bucket->Insert(origin_array[i].first, origin_array[i].second, comparator_));
    }
  }
  delete[] origin_array;

  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_index);
  for (uint32_t i = split_bucket_index; i >= diff; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }

  for (uint32_t i = split_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }

  for (uint32_t i = split_image_bucket_index; i >= diff; i -= diff) {
    dir_page->SetBucketPageId(i, image_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }

  for (uint32_t i = split_image_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, image_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }

  image_bucket_page->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(image_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));

  table_latch_.WUnlock();

  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);

  bool ret = bucket->Remove(key, value, comparator_);

  if (bucket->IsEmpty()) {
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    return ret;
  }

  // Unpin
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t target_bucket_index = KeyToDirectoryIndex(key, dir_page);
  page_id_t target_bucket_page_id = dir_page->GetBucketPageId(target_bucket_index);
  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(target_bucket_index);

  uint32_t local_depth = dir_page->GetLocalDepth(target_bucket_index);
  if (local_depth == 0) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  if (local_depth != dir_page->GetLocalDepth(image_bucket_index)) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  HASH_TABLE_BUCKET_TYPE *target_bucket = FetchBucketPage(target_bucket_page_id);
  if (!target_bucket->IsEmpty()) {
    assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(target_bucket_page_id));

  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_index);
  dir_page->SetBucketPageId(target_bucket_index, image_bucket_page_id);
  dir_page->DecrLocalDepth(target_bucket_index);
  dir_page->DecrLocalDepth(image_bucket_index);
  assert(dir_page->GetLocalDepth(target_bucket_index) == dir_page->GetLocalDepth(image_bucket_index));

  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == target_bucket_page_id || dir_page->GetBucketPageId(i) == image_bucket_page_id) {
      dir_page->SetBucketPageId(i, image_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(target_bucket_index));
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
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
template class DiskExtendibleHashTable<int, int, IntComparator>;

template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
