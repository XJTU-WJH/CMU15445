/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index)
    : buffer_pool_manager_(buffer_pool_manager), page_id_(page_id), index_(index) {
  leaf_page_ = nullptr;
  if (page_id_ != INVALID_PAGE_ID) {
    leaf_page_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_id_ != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  if (index_ == leaf_page_->GetSize() - 1 && leaf_page_->GetNextPageId() == INVALID_PAGE_ID) {
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  static KeyType key;
  static ValueType value;
  static MappingType temp(key, value);
  if (page_id_ == INVALID_PAGE_ID) {
    return temp;
  }
  return *(leaf_page_->ArrPtr(index_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_id_ == INVALID_PAGE_ID) {
    throw std::runtime_error("page_id is invalid!");
  }
  if (index_ < leaf_page_->GetSize() - 1) {
    index_++;
  } else {
    page_id_t next = leaf_page_->GetNextPageId();
    if (next == INVALID_PAGE_ID) {
      index_++;
    } else {
      buffer_pool_manager_->UnpinPage(page_id_, false);
      leaf_page_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next)->GetData());
      page_id_ = next;
      index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
