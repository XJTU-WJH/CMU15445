//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(BufferPoolManager *buffer_pool_manager = nullptr, page_id_t page_id = INVALID_PAGE_ID,
                         int index = 0);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (page_id_ == INVALID_PAGE_ID) {
      return itr.page_id_ == INVALID_PAGE_ID;
    }
    return page_id_ == itr.page_id_ && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  LeafPage *leaf_page_;
  page_id_t page_id_;
  int index_;
};

}  // namespace bustub
