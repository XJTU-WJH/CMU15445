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
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  const int init_depth = 0;
  global_depth_ = init_depth;
  num_buckets_ = (1 << init_depth);
  for (int i = 0; i < num_buckets_; i++) {
    dir_.push_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
  }
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
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  bool flag = dir_[index]->Insert(key, value);
  if (flag) {
    return;
  }
  while (dir_[index]->IsFull()) {
    ReallocateBucket(index, dir_[index]);
    index = IndexOf(key);
  }
  dir_[index]->Insert(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ReallocateBucket(size_t index, std::shared_ptr<Bucket> bucket) -> void {
  if (bucket->GetDepth() == global_depth_) {
    global_depth_++;
    bucket->IncrementDepth();
    num_buckets_++;
    int beg = (1 << (global_depth_ - 1));
    int end = (1 << (global_depth_));
    for (int i = beg; i < end; i++) {
      if (i == static_cast<int>(index + beg)) {
        dir_.push_back(std::make_shared<Bucket>(bucket_size_, bucket->GetDepth()));
      } else {
        dir_.push_back(dir_[i - beg]);
      }
    }
    Reinsert(dir_[index]);
  } else {
    Split(index, dir_[index]);
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Split(size_t index, std::shared_ptr<Bucket> bucket) {
  int dep = bucket->GetDepth();
  size_t mask = (index & ((1 << dep) - 1));
  mask |= (1 << dep);
  num_buckets_++;
  bucket->IncrementDepth();
  dep++;
  std::shared_ptr<Bucket> ptr = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  size_t lim = (1 << (global_depth_ - bucket->GetDepth()));
  for (size_t i = 0; i < lim; i++) {
    size_t val = ((i << dep) | mask);
    dir_[val] = ptr;
  }
  Reinsert(bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Reinsert(std::shared_ptr<Bucket> bucket) -> void {
  std::list<std::pair<K, V>> temp;
  temp.swap(bucket->GetItems());
  for (auto &it : temp) {
    size_t index = IndexOf(it.first);
    dir_[index]->Insert(it.first, it.second);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &it : list_) {
    if (it.first == key) {
      value = it.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &it : list_) {
    if (it.first == key) {
      it.second = value;
      return true;
    }
  }
  if (!IsFull()) {
    list_.emplace_back(std::pair<K, V>(key, value));
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
