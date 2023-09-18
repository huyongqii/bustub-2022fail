//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/config.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
#include "type/value.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      dir_.emplace_back(std::make_shared<Bucket>(bucket_size));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while(true) {
    size_t idx = IndexOf(key);
    bool flag = dir_[idx]->Insert(key,value);
    if(flag) {
      break;
    }
    if(GetGlobalDepthInternal() == GetLocalDepthInternal(idx)) {
      global_depth_++;
      size_t dir_size = dir_.size();
      // extend dir to dir * 2
      for(size_t i = 0; i < dir_size; i++) {
        dir_.emplace_back(dir_[i]);
      }
    } else {
      RedistributeBucket(dir_[idx]);
    }
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket)->void {
  bucket->IncrementDepth();
  int depth = bucket->GetDepth();
  num_buckets_++;
  auto list  = &bucket->GetItems();
  // create a new and image bucket
  std::shared_ptr<Bucket> image_bucket(new Bucket(bucket_size_, depth));
  auto pre = std::hash<K>()(list->begin()->first) & ((1 << (depth - 1)) - 1);
  for(auto it = list->begin(); it != list->end();) {
    // compute per element index
    auto cur = std::hash<K>()(it->first) & ((1 << depth) - 1);
    // it's same
    if(cur == pre) {
      ++it;
    } else {
      // insert key/value to image bucket
      image_bucket->Insert(it->first, it->second);
      list->erase(it++);
    }
  }
  // have a better way that update above loop
  for(size_t i = 0; i < dir_.size(); i++) {
    if((i & ((1 << (depth - 1)) - 1)) == pre && (i & (1 << depth) - 1) != pre) {
      dir_[i] = image_bucket;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(auto &[k,v] : list_) {
    if(k == key){
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for(auto it = list_.begin(); it != list_.end();) {
    if(it->first == key) {
      list_.erase(it);
      return true;
    }
    ++it;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for(auto &[k,v] : list_) {
    if(k == key) {
      v = value;
      return true;
    }
  }
  if(IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
