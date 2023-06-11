//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  if (static_cast<uint64_t>(max_size) > LEAF_PAGE_SIZE) {
    max_size = LEAF_PAGE_SIZE;
  }
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
  SetSize(0);
  SetLSN();
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return (array_ + index)->first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return (array_ + index)->second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { (array_ + index)->first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { (array_ + index)->second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindInsertPos(const KeyType &key, const KeyComparator &cmp) const -> int {
  // int len = GetSize();
  // if (len == 0) return 0;
  // int i = 0;
  // for (;i < len; i++) {
  //   auto temp_key = KeyAt(i);
  //   int res = cmp(temp_key, key);
  //   if (res == 0) {
  //     return -1;
  //   }
  //   if (res > 0) {
  //     break;
  //   }
  // }
  // return i;
  if (GetSize() == 0) {
    return 0;
  }
  int l = 0;
  int r = GetSize() - 1;
  int middle = 0;
  while (l < r) {
    middle = l + (r - l) / 2;
    auto res = cmp(KeyAt(middle), key);
    if (res >= 0) {
      r = middle;
    } else {
      l = middle + 1;
    }
  }
  auto res = cmp(KeyAt(l), key);
  if (res == 0) {
    return -1;
  }
  if (res < 0) {
    l++;
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKeyPos(const KeyType &key, const KeyComparator &cmp) const -> int {
  // int len = GetSize();
  // int i = 0;
  // for (;i < len; i++) {
  //   auto temp_key = KeyAt(i);
  //   int res = cmp(temp_key, key);
  //   if (res == 0) {
  //     return i;
  //   }
  // }
  // return -1;
  if (GetSize() == 0) {
    return 0;
  }
  int l = 0;
  int r = GetSize() - 1;
  int middle = 0;
  while (l < r) {
    middle = l + (r - l) / 2;
    auto res = cmp(KeyAt(middle), key);
    if (res >= 0) {
      r = middle;
    } else {
      l = middle + 1;
    }
  }
  if (cmp(KeyAt(l), key) != 0) {
    return -1;
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveBack(int index) {
  char buff[BUSTUB_PAGE_SIZE] = {0};
  auto src_ptr = reinterpret_cast<void *>(ArrPtr(index));
  size_t num = GetSize() - index;
  if (num <= 0) {
    return;
  }
  memcpy(reinterpret_cast<void *>(buff), src_ptr, num * CellSize());
  auto dsc_ptr = reinterpret_cast<void *>(ArrPtr(index + 1));
  memcpy(dsc_ptr, reinterpret_cast<void *>(buff), num * CellSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveForward(int index) {
  if (index <= 0 || (GetSize() - index) <= 0) {
    DecreaseSize();
    return;
  }
  size_t num = GetSize() - index;
  auto src_ptr = ArrPtr(index);
  auto dst_ptr = ArrPtr(index - 1);
  memmove(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * CellSize());
  DecreaseSize();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
