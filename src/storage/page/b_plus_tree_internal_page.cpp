//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetParentPageId(parent_id);
  SetPageId(page_id);
  if (static_cast<uint64_t>(max_size) > INTERNAL_PAGE_SIZE) {
    max_size = INTERNAL_PAGE_SIZE;
  }
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetLSN();
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return (array_ + index)->first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { (array_ + index)->first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return (array_ + index)->second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { (array_ + index)->second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValuePos(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    auto val = ValueAt(i);
    if (val == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindKeyPos(const KeyType &key, const KeyComparator &cmp) const -> int {
  // for (int i = 1; i < GetSize(); i++) {
  //   auto temp = KeyAt(i);
  //   if (cmp(temp, key) == 0) {
  //     return i;
  //   }
  // }
  // return 0;
  int l = 1;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveBack(int index) {
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveForward(int index) {
  if (index <= 0) {
    throw std::logic_error("internal page moveforward error!");
    return;
  }
  size_t num = GetSize() - index;
  if (num <= 0) {
    DecreaseSize();
    return;
  }
  auto src_ptr = ArrPtr(index);
  auto dst_ptr = ArrPtr(index - 1);
  memmove(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * CellSize());
  DecreaseSize();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
