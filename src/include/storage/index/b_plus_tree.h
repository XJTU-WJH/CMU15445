//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <algorithm>
#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  auto BuildTree() -> bool;

  auto DfsFindPage(const KeyType &key, page_id_t current_page_id, page_id_t &leaf_page_id,
                   Transaction *transaction = nullptr, bool is_write = false) -> LeafPage *;

  auto InsertLeaf(LeafPage *page_ptr, int insert_pos, const KeyType &key, const ValueType &value,
                  Transaction *transaction = nullptr) -> bool;

  auto InsertParent(BPlusTreePage *page_ptr, const KeyType &key, BPlusTreePage *new_page_ptr,
                    Transaction *transaction = nullptr) -> bool;

  void ReallocatLeafPage(LeafPage *page_ptr, LeafPage *new_page_ptr, int insert_pos, const KeyType &key,
                         const ValueType &value, Transaction *transaction = nullptr);

  void ReallocateInternalPage(Transaction *transaction = nullptr);

  void DeleteEntry(BPlusTreePage *page_ptr, const KeyType &key, int delete_pos, Transaction *transaction = nullptr);

  auto FindBrother(BPlusTreePage *page, page_id_t parent_page_id, BPlusTreePage *&lef_bro, BPlusTreePage *&rig_bro,
                   Page *&bro_ptr, Transaction *transaction = nullptr) -> KeyType;

  void Merge(BPlusTreePage *lef_page_ptr, const KeyType &key, BPlusTreePage *rig_page_ptr, Page *bro_ptr,
             Transaction *transaction = nullptr);

  void ReallocateInRemoveLeft(BPlusTreePage *lef_page_ptr, const KeyType &key, BPlusTreePage *rig_page_ptr,
                              Transaction *transaction = nullptr);

  void ReallocateInRemoveRight(BPlusTreePage *lef_page_ptr, const KeyType &key, BPlusTreePage *rig_page_ptr,
                               Transaction *transaction = nullptr);

  void RemoveAllLock(Transaction *transaction, bool is_write = false);

  static auto Ceil(int a, int b) -> int { return a % b == 0 ? a / b : (a / b + 1); }

  auto BinarySearch(int l, int r, const KeyType &key, InternalPage *ptr) -> int;

  auto FindIf(int l, int r, const KeyType &key, LeafPage *ptr) -> int;

  auto FindValue(BPlusTreePage *page, InternalPage *pare) -> int;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  ReaderWriterLatch latch_;
  std::mutex mtx_;
  int insert_count_;
  int remove_count_;
};

}  // namespace bustub
