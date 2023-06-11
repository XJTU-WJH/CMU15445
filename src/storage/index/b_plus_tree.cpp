#include <algorithm>
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  insert_count_ = 0;
  remove_count_ = 0;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  std::scoped_lock<std::mutex> latch(mtx_);
  latch_.RLock();
  page_id_t page = INVALID_PAGE_ID;
  LeafPage *page_ptr = DfsFindPage(key, root_page_id_, page, transaction);
  if (page_ptr == nullptr) {
    return false;
  }
  int size = page_ptr->GetSize();
  Page *last_leaf_page = buffer_pool_manager_->FetchPage(page_ptr->GetPageId());
  int i = FindIf(0, size, key, page_ptr);
  bool flag = false;
  if (i != -1) {
    result->push_back(page_ptr->ValueAt(i));
    flag = true;
  }
  buffer_pool_manager_->UnpinPage(last_leaf_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(last_leaf_page->GetPageId(), false);
  last_leaf_page->RUnlatch();
  return flag;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveAllLock(Transaction *transaction, bool is_write) {
  auto page_set = transaction->GetPageSet();
  auto delete_set = transaction->GetDeletedPageSet();
  bool flag = page_set->empty();
  if (is_write) {
    while (!flag) {
      Page *ptr = page_set->back();
      page_set->pop_back();
      page_id_t id = ptr->GetPageId();
      if (delete_set->count(id) != 0) {
        buffer_pool_manager_->UnpinPage(id, true);
        buffer_pool_manager_->DeletePage(id);
      } else {
        buffer_pool_manager_->UnpinPage(id, true);
      }
      if (page_set->empty()) {
        delete_set->clear();
      }
      flag = page_set->empty();
      ptr->WUnlatch();
    }
  } else {
    while (!flag) {
      Page *ptr = page_set->back();
      page_set->pop_back();
      page_id_t id = ptr->GetPageId();
      if (delete_set->count(id) != 0) {
        buffer_pool_manager_->UnpinPage(id, false);
        buffer_pool_manager_->DeletePage(id);
      } else {
        buffer_pool_manager_->UnpinPage(id, false);
      }
      if (page_set->empty()) {
        delete_set->clear();
      }
      flag = page_set->empty();
      ptr->RUnlatch();
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(int l, int r, const KeyType &key, InternalPage *ptr) -> int {
  int lim = r;
  r--;
  int middle = 0;
  while (l < r) {
    middle = l + (r - l) / 2;
    auto res = comparator_(ptr->KeyAt(middle), key);
    if (res >= 0) {
      r = middle;
    } else {
      l = middle + 1;
    }
  }
  if (comparator_(ptr->KeyAt(l), key) > 0) {
    l--;
  }
  if (l == lim) {
    l--;
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindIf(int l, int r, const KeyType &key, LeafPage *ptr) -> int {
  r--;
  int middle = 0;
  while (l < r) {
    middle = l + (r - l) / 2;
    auto res = comparator_(ptr->KeyAt(middle), key);
    if (res >= 0) {
      r = middle;
    } else {
      l = middle + 1;
    }
  }
  if (comparator_(ptr->KeyAt(l), key) != 0) {
    return -1;
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindValue(BPlusTreePage *page, InternalPage *pare_page) -> int {
  int pos = 0;
  if (page->IsLeafPage()) {
    pos = BinarySearch(1, pare_page->GetSize(), (reinterpret_cast<LeafPage *>(page))->KeyAt(0),
                       reinterpret_cast<InternalPage *>(pare_page));
  } else {
    pos = BinarySearch(1, pare_page->GetSize(), (reinterpret_cast<InternalPage *>(page))->KeyAt(1),
                       reinterpret_cast<InternalPage *>(pare_page));
  }
  return pos;
}

// 查找叶节点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DfsFindPage(const KeyType &key, page_id_t current_page_id, page_id_t &leaf_page_id,
                                 Transaction *transaction, bool is_write) -> LeafPage * {
  if (current_page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  Page *ptr = buffer_pool_manager_->FetchPage(current_page_id);
  if (is_write) {
    ptr->WLatch();
  } else {
    ptr->RLatch();
  }
  auto page_p = reinterpret_cast<BPlusTreePage *>(ptr->GetData());
  if (!(page_p->IsLeafPage())) {
    auto page_ptr = reinterpret_cast<InternalPage *>(page_p);
    int size = page_ptr->GetSize();
    int i = BinarySearch(1, size, key, page_ptr);
    page_id_t next_page = page_ptr->ValueAt(i);
    if (current_page_id == root_page_id_ && !is_write) {
      latch_.RUnlock();
    }
    if (!is_write) {
      ptr->RUnlatch();
      buffer_pool_manager_->UnpinPage(current_page_id, false);
    } else {
      transaction->AddIntoPageSet(ptr);
    }
    return DfsFindPage(key, next_page, leaf_page_id, transaction, is_write);
  }
  if (current_page_id == root_page_id_ && !is_write) {
    latch_.RUnlock();
  }
  if (is_write) {
    transaction->AddIntoPageSet(ptr);
  }
  leaf_page_id = page_p->GetPageId();
  return reinterpret_cast<LeafPage *>(page_p);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BuildTree() -> bool {
  if (root_page_id_ != INVALID_PAGE_ID) {
    return true;
  }
  page_id_t id = INVALID_PAGE_ID;
  Page *page_ptr = buffer_pool_manager_->NewPage(&id);
  if (page_ptr == nullptr) {
    return false;
  }
  root_page_id_ = page_ptr->GetPageId();
  auto page = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  UpdateRootPageId();
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  std::scoped_lock<std::mutex> latch(mtx_);
  latch_.WLock();
  insert_count_++;
  if (root_page_id_ == INVALID_PAGE_ID && !BuildTree()) {
    latch_.WUnlock();
    return false;
  }
  page_id_t page = INVALID_PAGE_ID;
  LeafPage *page_ptr = DfsFindPage(key, root_page_id_, page, transaction, true);
  if (page_ptr == nullptr) {
    latch_.WUnlock();
    throw std::logic_error("DfsFindPage return nullptr!");
    return false;
  }
  int pos = page_ptr->FindInsertPos(key, comparator_);
  if (pos == -1) {
    latch_.WUnlock();
    RemoveAllLock(transaction, true);
    return false;
  }
  int size = page_ptr->GetSize();
  if (size < page_ptr->GetMaxSize()) {
    return InsertLeaf(page_ptr, pos, key, value, transaction);
  }
  page_id_t id = INVALID_PAGE_ID;
  Page *new_page_ptr = buffer_pool_manager_->NewPage(&id);
  auto new_leaf_ptr = reinterpret_cast<LeafPage *>(new_page_ptr->GetData());
  new_leaf_ptr->Init(new_page_ptr->GetPageId(), INVALID_PAGE_ID, leaf_max_size_);
  ReallocatLeafPage(page_ptr, new_leaf_ptr, pos, key, value, transaction);
  return InsertParent(page_ptr, new_leaf_ptr->KeyAt(0), new_leaf_ptr, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReallocatLeafPage(LeafPage *page_ptr, LeafPage *new_page_ptr, int insert_pos, const KeyType &key,
                                       const ValueType &value, Transaction *transaction) {
  int size = page_ptr->GetSize();
  int len = Ceil(size + 1, 2);
  int new_size = size + 1 - len;
  new_page_ptr->SetSize(new_size);
  page_ptr->SetSize(len);
  if (insert_pos < len) {
    auto src_ptr = page_ptr->ArrPtr(len - 1);
    auto dst_ptr = new_page_ptr->ArrPtr(0);
    size_t num = new_size;
    if (num > 0) {
      memcpy(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * page_ptr->CellSize());
    }
    page_ptr->MoveBack(insert_pos);
    page_ptr->SetKeyAt(insert_pos, key);
    page_ptr->SetValueAt(insert_pos, value);
  } else {
    auto src_ptr = page_ptr->ArrPtr(len);
    auto dst_ptr = new_page_ptr->ArrPtr(0);
    size_t num = insert_pos - len;
    if (num > 0) {
      memcpy(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * page_ptr->CellSize());
    }
    src_ptr = page_ptr->ArrPtr(insert_pos);
    dst_ptr = new_page_ptr->ArrPtr(insert_pos - len + 1);
    num = size - insert_pos;
    if (num > 0) {
      memcpy(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * page_ptr->CellSize());
    }
    new_page_ptr->SetKeyAt(insert_pos - len, key);
    new_page_ptr->SetValueAt(insert_pos - len, value);
  }
  new_page_ptr->SetNextPageId(page_ptr->GetNextPageId());
  page_ptr->SetNextPageId(new_page_ptr->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertLeaf(LeafPage *page_ptr, int insert_pos, const KeyType &key, const ValueType &value,
                                Transaction *transaction) -> bool {
  page_ptr->MoveBack(insert_pos);
  page_ptr->SetKeyAt(insert_pos, key);
  page_ptr->SetValueAt(insert_pos, value);
  page_ptr->IncreaseSize();
  latch_.WUnlock();
  RemoveAllLock(transaction, true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertParent(BPlusTreePage *page_ptr, const KeyType &key, BPlusTreePage *new_page_ptr,
                                  Transaction *transaction) -> bool {
  if (page_ptr->GetPageId() == root_page_id_) {
    page_id_t id = INVALID_PAGE_ID;
    Page *page = buffer_pool_manager_->NewPage(&id);
    auto new_page = reinterpret_cast<InternalPage *>(page->GetData());
    new_page->Init(page->GetPageId(), INVALID_PAGE_ID, internal_max_size_);
    new_page->SetKeyAt(1, key);
    new_page->SetValueAt(0, page_ptr->GetPageId());
    new_page->SetValueAt(1, new_page_ptr->GetPageId());
    new_page->SetSize(2);
    page_ptr->SetParentPageId(new_page->GetPageId());
    new_page_ptr->SetParentPageId(new_page->GetPageId());
    root_page_id_ = id;
    // latch_.WUnlock();
    // RemoveAllLock(transaction, true);
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(new_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    latch_.WUnlock();
    RemoveAllLock(transaction, true);
    return true;
  }
  auto page =
      reinterpret_cast<InternalPage *>((buffer_pool_manager_->FetchPage(page_ptr->GetParentPageId()))->GetData());
  int size = page->GetSize();
  int i = 0;
  if (page_ptr->IsLeafPage()) {
    i = BinarySearch(0, size, (reinterpret_cast<LeafPage *>(page_ptr))->KeyAt(page_ptr->GetSize() - 1), page);
  } else {
    i = BinarySearch(0, size, (reinterpret_cast<InternalPage *>(page_ptr))->KeyAt(page_ptr->GetSize() - 1), page);
  }
  if (size < page->GetMaxSize()) {
    page->MoveBack(i + 1);
    page->SetKeyAt(i + 1, key);
    page->SetValueAt(i + 1, new_page_ptr->GetPageId());
    page->IncreaseSize();
    page_ptr->SetParentPageId(page->GetPageId());
    new_page_ptr->SetParentPageId(page->GetPageId());
    buffer_pool_manager_->UnpinPage(new_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    latch_.WUnlock();
    RemoveAllLock(transaction, true);
    return true;
  }
  page_id_t id = INVALID_PAGE_ID;
  Page *temp_page = buffer_pool_manager_->NewPage(&id);
  auto new_inter_page = reinterpret_cast<InternalPage *>(temp_page->GetData());
  new_inter_page->Init(temp_page->GetPageId(), INVALID_PAGE_ID, internal_max_size_);
  KeyType temp;
  int len = Ceil(size + 1, 2);
  int new_size = size + 1 - len;
  i++;
  if (i < len) {
    auto src_ptr = page->ArrPtr(len - 1);
    auto dst_ptr = new_inter_page->ArrPtr(0);
    size_t num = size + 1 - len;
    if (num > 0) {
      memcpy(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * page->CellSize());
    }
    new_inter_page->SetSize(new_size);
    page->SetSize(len);
    temp = page->KeyAt(len - 1);
    page->MoveBack(i);
    page->SetKeyAt(i, key);
    page->SetValueAt(i, new_page_ptr->GetPageId());
    new_page_ptr->SetParentPageId(page->GetPageId());
    for (int j = 0; j < new_size; j++) {
      auto p =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(new_inter_page->ValueAt(j))->GetData());
      p->SetParentPageId(id);
      buffer_pool_manager_->UnpinPage(p->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(new_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    InsertParent(page, temp, new_inter_page, transaction);
  } else if (i == len) {
    auto src_ptr = page->ArrPtr(len);
    auto dst_ptr = new_inter_page->ArrPtr(1);
    size_t num = size - len;
    if (num > 0) {
      memcpy(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * page->CellSize());
    }
    new_inter_page->SetValueAt(0, new_page_ptr->GetPageId());
    new_inter_page->SetSize(new_size);
    new_page_ptr->SetParentPageId(page->GetPageId());
    for (int j = 0; j < new_size; j++) {
      auto p =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(new_inter_page->ValueAt(j))->GetData());
      p->SetParentPageId(id);
      buffer_pool_manager_->UnpinPage(p->GetPageId(), true);
    }
    page->SetSize(len);
    buffer_pool_manager_->UnpinPage(new_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    InsertParent(page, key, new_inter_page, transaction);
  } else {
    auto src_ptr = page->ArrPtr(len);
    auto dst_ptr = new_inter_page->ArrPtr(0);
    size_t num = size - len;
    if (num > 0) {
      memcpy(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * page->CellSize());
    }
    page->SetSize(len);
    new_inter_page->SetSize(new_size - 1);
    int pos = i - len;
    new_inter_page->MoveBack(pos);
    temp = page->KeyAt(len);
    new_inter_page->SetKeyAt(pos, key);
    new_inter_page->SetValueAt(pos, new_page_ptr->GetPageId());
    new_inter_page->IncreaseSize();
    new_page_ptr->SetParentPageId(new_inter_page->GetPageId());
    for (int j = 0; j < new_size; j++) {
      auto p =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(new_inter_page->ValueAt(j))->GetData());
      p->SetParentPageId(id);
      buffer_pool_manager_->UnpinPage(p->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(new_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    InsertParent(page, temp, new_inter_page, transaction);
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  std::scoped_lock<std::mutex> latch(mtx_);
  latch_.WLock();
  remove_count_++;
  if (root_page_id_ == INVALID_PAGE_ID) {
    latch_.WUnlock();
    return;
  }
  page_id_t leaf_page_id;
  LeafPage *leaf_page = DfsFindPage(key, root_page_id_, leaf_page_id, transaction, true);
  if (leaf_page == nullptr) {
    latch_.WUnlock();
    return;
  }
  int pos = leaf_page->FindKeyPos(key, comparator_);
  if (pos == -1) {
    latch_.WUnlock();
    RemoveAllLock(transaction, true);
    return;
  }
  DeleteEntry(reinterpret_cast<BPlusTreePage *>(leaf_page), key, pos, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(BPlusTreePage *page_ptr, const KeyType &key, int delete_pos,
                                 Transaction *transaction) {
  if (page_ptr->IsLeafPage()) {
    auto leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr);
    leaf_page_ptr->MoveForward(delete_pos + 1);
  } else {
    auto inter_page_ptr = reinterpret_cast<InternalPage *>(page_ptr);
    inter_page_ptr->MoveForward(delete_pos + 1);
  }
  if (page_ptr->GetPageId() == root_page_id_) {
    if (!page_ptr->IsLeafPage()) {
      if (page_ptr->GetSize() == 1) {
        auto id = (reinterpret_cast<InternalPage *>(page_ptr))->ValueAt(0);
        auto child_page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(id)->GetData());
        root_page_id_ = child_page_ptr->GetPageId();
        child_page_ptr->SetParentPageId(INVALID_PAGE_ID);
        transaction->AddIntoDeletedPageSet(page_ptr->GetPageId());
        latch_.WUnlock();
        buffer_pool_manager_->UnpinPage(id, true);
        RemoveAllLock(transaction, true);
        UpdateRootPageId();
        return;
      }
    } else {
      if (page_ptr->GetSize() == 0) {
        root_page_id_ = INVALID_PAGE_ID;
        transaction->AddIntoDeletedPageSet(page_ptr->GetPageId());
        latch_.WUnlock();
        RemoveAllLock(transaction, true);
        UpdateRootPageId();
        return;
      }
    }
    latch_.WUnlock();
    RemoveAllLock(transaction, true);
    return;
  }
  int lim = Ceil(page_ptr->GetMaxSize(), 2);
  if (page_ptr->GetSize() < lim) {
    BPlusTreePage *lef_bro = nullptr;
    BPlusTreePage *rig_bro = nullptr;
    BPlusTreePage *bro = nullptr;
    Page *bro_ptr = nullptr;
    KeyType temp_key = FindBrother(page_ptr, page_ptr->GetParentPageId(), lef_bro, rig_bro, bro_ptr, transaction);
    if (rig_bro != nullptr) {
      bro = rig_bro;
      if (lef_bro != nullptr) {
        buffer_pool_manager_->UnpinPage(lef_bro->GetPageId(), false);
      }
    } else if (lef_bro != nullptr) {
      bro = lef_bro;
    }
    if (bro == nullptr) {
      throw std::logic_error("Can't find brother when remove!");
      return;
    }
    if (page_ptr->GetSize() + bro->GetSize() <= page_ptr->GetMaxSize()) {
      if (bro == rig_bro) {
        Merge(page_ptr, temp_key, bro, bro_ptr, transaction);
      } else {
        Merge(bro, temp_key, page_ptr, bro_ptr, transaction);
      }
    } else {
      if (bro == rig_bro) {
        ReallocateInRemoveRight(page_ptr, temp_key, bro, transaction);
      } else {
        ReallocateInRemoveLeft(bro, temp_key, page_ptr, transaction);
      }
    }
  } else {
    latch_.WUnlock();
    RemoveAllLock(transaction, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *lef_page_ptr, const KeyType &key, BPlusTreePage *rig_page_ptr, Page *bro_ptr,
                           Transaction *transaction) {
  int pos = lef_page_ptr->GetSize();
  if (!lef_page_ptr->IsLeafPage()) {
    size_t cell_size = (reinterpret_cast<InternalPage *>(rig_page_ptr))->CellSize();
    auto src_ptr = (reinterpret_cast<InternalPage *>(rig_page_ptr))->ArrPtr(0);
    auto dst_ptr = (reinterpret_cast<InternalPage *>(lef_page_ptr))->ArrPtr(pos);
    int num = rig_page_ptr->GetSize();
    for (int j = 0; j < num; j++) {
      auto p = reinterpret_cast<BPlusTreePage *>(
          buffer_pool_manager_->FetchPage((reinterpret_cast<InternalPage *>(rig_page_ptr))->ValueAt(j))->GetData());
      p->SetParentPageId(lef_page_ptr->GetPageId());
      buffer_pool_manager_->UnpinPage(p->GetPageId(), true);
    }
    memcpy(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * cell_size);
    (reinterpret_cast<InternalPage *>(lef_page_ptr))->SetKeyAt(pos, key);
    lef_page_ptr->SetSize(pos + num);
  } else {
    size_t cell_size = (reinterpret_cast<LeafPage *>(rig_page_ptr))->CellSize();
    auto lef_page = reinterpret_cast<LeafPage *>(lef_page_ptr);
    auto rig_page = reinterpret_cast<LeafPage *>(rig_page_ptr);
    lef_page->SetNextPageId(rig_page->GetNextPageId());
    auto src_ptr = rig_page->ArrPtr(0);
    auto dst_ptr = lef_page->ArrPtr(pos);
    size_t num = rig_page->GetSize();
    memcpy(reinterpret_cast<void *>(dst_ptr), reinterpret_cast<void *>(src_ptr), num * cell_size);
    lef_page->SetSize(pos + num);
  }
  auto pare_page =
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(lef_page_ptr->GetParentPageId())->GetData());
  transaction->AddIntoPageSet(bro_ptr);
  bro_ptr->WLatch();
  transaction->AddIntoDeletedPageSet(rig_page_ptr->GetPageId());
  buffer_pool_manager_->UnpinPage(pare_page->GetPageId(), true);
  int delete_pos = FindValue(rig_page_ptr, reinterpret_cast<InternalPage *>(pare_page));
  if (!lef_page_ptr->IsLeafPage()) {
    (reinterpret_cast<InternalPage *>(lef_page_ptr))
        ->SetKeyAt(pos, (reinterpret_cast<InternalPage *>(pare_page))->KeyAt(delete_pos));
  }
  DeleteEntry(pare_page, key, delete_pos, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReallocateInRemoveLeft(BPlusTreePage *lef_page_ptr, const KeyType &key,
                                            BPlusTreePage *rig_page_ptr, Transaction *transaction) {
  KeyType temp;
  page_id_t pare_id = INVALID_PAGE_ID;
  if (!lef_page_ptr->IsLeafPage()) {
    auto lef_page = reinterpret_cast<InternalPage *>(lef_page_ptr);
    auto rig_page = reinterpret_cast<InternalPage *>(rig_page_ptr);
    pare_id = rig_page->GetParentPageId();
    temp = lef_page->KeyAt(lef_page->GetSize() - 1);
    rig_page->MoveBack(0);
    rig_page->SetKeyAt(1, key);
    page_id_t id = lef_page->ValueAt(lef_page->GetSize() - 1);
    auto p = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(id)->GetData());
    p->SetParentPageId(rig_page->GetPageId());
    buffer_pool_manager_->UnpinPage(id, true);
    rig_page->SetValueAt(0, id);
    rig_page->IncreaseSize();
    lef_page->DecreaseSize();
  } else {
    auto lef_page = reinterpret_cast<LeafPage *>(lef_page_ptr);
    auto rig_page = reinterpret_cast<LeafPage *>(rig_page_ptr);
    pare_id = rig_page->GetParentPageId();
    temp = lef_page->KeyAt(lef_page->GetSize() - 1);
    rig_page->MoveBack(0);
    rig_page->SetKeyAt(0, lef_page->KeyAt(lef_page->GetSize() - 1));
    rig_page->SetValueAt(0, lef_page->ValueAt(lef_page->GetSize() - 1));
    rig_page->IncreaseSize();
    lef_page->DecreaseSize();
  }
  auto pare_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(pare_id)->GetData());
  int pos = pare_page->FindKeyPos(key, comparator_);
  if (pos == -1) {
    throw std::logic_error("Internal page find key pos error! not found");
  }
  pare_page->SetKeyAt(pos, temp);
  buffer_pool_manager_->UnpinPage(lef_page_ptr->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(pare_page->GetPageId(), true);
  latch_.WUnlock();
  RemoveAllLock(transaction, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReallocateInRemoveRight(BPlusTreePage *lef_page_ptr, const KeyType &key,
                                             BPlusTreePage *rig_page_ptr, Transaction *transaction) {
  KeyType temp;
  page_id_t pare_id = INVALID_PAGE_ID;
  if (!rig_page_ptr->IsLeafPage()) {
    auto lef_page = reinterpret_cast<InternalPage *>(lef_page_ptr);
    auto rig_page = reinterpret_cast<InternalPage *>(rig_page_ptr);
    pare_id = rig_page->GetParentPageId();
    temp = rig_page->KeyAt(1);
    lef_page->SetKeyAt(lef_page->GetSize(), key);
    page_id_t id = rig_page->ValueAt(0);
    auto p = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(id)->GetData());
    p->SetParentPageId(lef_page->GetPageId());
    buffer_pool_manager_->UnpinPage(id, true);
    lef_page->SetValueAt(lef_page->GetSize(), id);
    lef_page->IncreaseSize();
    rig_page->MoveForward(1);
  } else {
    auto lef_page = reinterpret_cast<LeafPage *>(lef_page_ptr);
    auto rig_page = reinterpret_cast<LeafPage *>(rig_page_ptr);
    pare_id = rig_page->GetParentPageId();
    lef_page->SetKeyAt(lef_page->GetSize(), rig_page->KeyAt(0));
    lef_page->SetValueAt(lef_page->GetSize(), rig_page->ValueAt(0));
    lef_page->IncreaseSize();
    rig_page->MoveForward(1);
    temp = rig_page->KeyAt(0);
  }
  auto pare_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(pare_id)->GetData());
  int pos = pare_page->FindKeyPos(key, comparator_);
  if (pos == -1) {
    throw std::logic_error("Internal page find key pos error! not found");
  }
  pare_page->SetKeyAt(pos, temp);
  buffer_pool_manager_->UnpinPage(rig_page_ptr->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(pare_page->GetPageId(), true);
  latch_.WUnlock();
  RemoveAllLock(transaction, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindBrother(BPlusTreePage *page, page_id_t parent_page_id, BPlusTreePage *&lef_bro,
                                 BPlusTreePage *&rig_bro, Page *&bro_ptr, Transaction *transaction) -> KeyType {
  lef_bro = nullptr;
  rig_bro = nullptr;
  if (parent_page_id == INVALID_PAGE_ID) {
    throw std::logic_error("FindBrother error due to the parent is INVALID");
    return KeyType();
  }
  auto parent_page_ptr = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  int pos = FindValue(page, parent_page_ptr);
  {
    page_id_t temp = parent_page_ptr->ValueAt(pos);
    if (temp != page->GetPageId()) {
      throw std::logic_error("FindBrother error due to pos incorrect");
    }
  }
  int size = parent_page_ptr->GetSize();
  if (pos < size - 1) {
    page_id_t id = parent_page_ptr->ValueAt(pos + 1);
    Page *ptr = buffer_pool_manager_->FetchPage(id);
    rig_bro = reinterpret_cast<BPlusTreePage *>(ptr->GetData());
    KeyType key = parent_page_ptr->KeyAt(pos + 1);
    buffer_pool_manager_->UnpinPage(parent_page_id, false);
    bro_ptr = ptr;
    return key;
  }
  if (pos > 0) {
    page_id_t id = parent_page_ptr->ValueAt(pos - 1);
    Page *ptr = buffer_pool_manager_->FetchPage(id);
    lef_bro = reinterpret_cast<BPlusTreePage *>(ptr->GetData());
    KeyType key = parent_page_ptr->KeyAt(pos);
    buffer_pool_manager_->UnpinPage(parent_page_id, false);
    bro_ptr = ptr;
    return key;
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  return KeyType();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  std::scoped_lock<std::mutex> latch(mtx_);
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  auto page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!(page_ptr->IsLeafPage())) {
    page_id_t child_page_id = (reinterpret_cast<InternalPage *>(page_ptr))->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
  }
  page_id_t id = page_ptr->GetPageId();
  buffer_pool_manager_->UnpinPage(id, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, id);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::scoped_lock<std::mutex> latch(mtx_);
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  LeafPage *page_ptr = DfsFindPage(key, root_page_id_, leaf_page_id);
  if (page_ptr == nullptr) {
    return INDEXITERATOR_TYPE();
  }
  int pos = page_ptr->FindKeyPos(key, comparator_);
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  if (pos == -1) {
    return INDEXITERATOR_TYPE();
  }
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_id, pos);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  std::scoped_lock<std::mutex> latch(mtx_);
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  auto page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!(page_ptr->IsLeafPage())) {
    page_id_t child_page_id = (reinterpret_cast<InternalPage *>(page_ptr))->ValueAt(page_ptr->GetSize() - 1);
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id)->GetData());
  }
  page_id_t id = page_ptr->GetPageId();
  int index = page_ptr->GetSize();
  buffer_pool_manager_->UnpinPage(id, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, id, index);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
