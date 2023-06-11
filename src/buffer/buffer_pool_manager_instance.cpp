//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  return ReallocatePage(page_id);
}

auto BufferPoolManagerInstance::ReallocatePage(page_id_t *page_id, bool has_id) -> Page * {
  frame_id_t frame;
  {
    if (free_list_.empty()) {
      bool flag = replacer_->Evict(&frame);
      if (!flag) {
        return nullptr;
      }
      page_table_->Remove(pages_[frame].GetPageId());
      if (pages_[frame].IsDirty()) {
        disk_manager_->WritePage(pages_[frame].GetPageId(), pages_[frame].GetData());
      }
    } else {
      frame = *(free_list_.begin());
      free_list_.pop_front();
    }
  }
  if (!has_id) {
    *page_id = AllocatePage();
  }
  ResetPage(*page_id, frame);
  page_table_->Insert(*page_id, frame);
  replacer_->RecordAccess(frame);
  replacer_->SetEvictable(frame, false);
  return pages_ + frame;
}

void BufferPoolManagerInstance::ResetPage(page_id_t page, frame_id_t frame) {
  pages_[frame].page_id_ = page;
  pages_[frame].ResetMemory();
  pages_[frame].pin_count_ = 1;
  pages_[frame].is_dirty_ = false;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame;
  if (page_table_->Find(page_id, frame)) {
    replacer_->RecordAccess(frame);
    replacer_->SetEvictable(frame, false);
    pages_[frame].pin_count_++;
    return pages_ + frame;
  }
  Page *page_ptr = nullptr;
  page_ptr = ReallocatePage(&page_id, true);
  if (page_ptr != nullptr) {
    frame = page_ptr - pages_;
    ResetPage(page_id, frame);
    disk_manager_->ReadPage(page_id, page_ptr->GetData());
  }
  return page_ptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame;
  bool flag = page_table_->Find(page_id, frame);
  if (!flag) {
    return false;
  }
  if (pages_[frame].GetPinCount() == 0) {
    return false;
  }
  if (pages_[frame].GetPinCount() == 1) {
    replacer_->SetEvictable(frame, true);
  }
  if (!pages_[frame].is_dirty_) {
    pages_[frame].is_dirty_ = is_dirty;
  }
  pages_[frame].pin_count_--;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame;
  bool flag = page_table_->Find(page_id, frame);
  if (!flag) {
    return false;
  }
  disk_manager_->WritePage(pages_[frame].GetPageId(), pages_[frame].GetData());
  pages_[frame].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame;
  for (size_t i = 0; i < pool_size_; i++) {
    bool flag = page_table_->Find(pages_[i].GetPageId(), frame);
    if (flag && i == static_cast<size_t>(frame)) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame;
  bool flag = page_table_->Find(page_id, frame);
  if (!flag) {
    return true;
  }
  if (pages_[frame].GetPinCount() != 0) {
    return false;
  }
  replacer_->Remove(frame);
  page_table_->Remove(page_id);
  { free_list_.push_back(frame); }
  // ResetPage(page_id, frame);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
