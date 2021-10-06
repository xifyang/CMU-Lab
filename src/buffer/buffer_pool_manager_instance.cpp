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

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t flush_fid = page_table_[page_id];
  Page *flush_page = &pages_[flush_fid];
  disk_manager_->WritePage(flush_page->GetPageId(), flush_page->GetData());
  flush_page->is_dirty_ = false;
  LOG_INFO("Succeed to flush Page: page_id:%d, frame_id:%d", flush_page->GetPageId(), flush_fid);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::unique_lock<std::mutex> lock(latch_);
  for (auto &entry : page_table_) {
    Page *flush_page = &pages_[entry.second];
    disk_manager_->WritePage(flush_page->GetPageId(), flush_page->GetData());
    flush_page->is_dirty_ = false;
    LOG_INFO("Succeed to flush Page:%d, frame_id:%d", flush_page->GetPageId(), entry.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id_t new_pid = AllocatePage();
  frame_id_t victm_fid = -1;
  std::unique_lock<std::mutex> lock(latch_);
  LOG_INFO("Starting to allocate frame_id for page_id:%d", new_pid);
  do {
    // Pick a victim page P from either the free list
    if (!free_list_.empty()) {
      victm_fid = free_list_.front();
      free_list_.pop_front();
      LOG_INFO("Succeed to get frame_id:%d from free list for page_id:%d", victm_fid, new_pid);
      break;
    }
    // If all the pages in the buffer pool are pinned, return nullptr.
    bool is_all_spinned = true;
    for (int i = 0; i < static_cast<int>(pool_size_); i++) {
      if (pages_[i].GetPinCount() == 0) {
        is_all_spinned = false;
      }
    }
    if (is_all_spinned) {
      LOG_INFO("Failed to get frame for page_id:%d ,All page have been spinned", new_pid);
      break;
    }
    // Pick a victim page P from the replacer, erase P from the page table, write page if dirty
    if (!replacer_->Victim(&victm_fid)) {
      LOG_INFO("Failed to victim frame for page_id:%d", new_pid);
      break;
    }
    Page *replace_page = &pages_[victm_fid];
    if (replace_page->IsDirty()) {
      disk_manager_->WritePage(replace_page->GetPageId(), replace_page->GetData());
    }
    page_table_.erase(replace_page->page_id_);
    LOG_INFO("Succeed to victim frame_id:%d for page_id:%d", victm_fid, new_pid);
  } while (false);
  if (victm_fid == -1) {
    return nullptr;
  }
  // Add P to the page table, update P's metadata, zero out memory
  Page *victm_page = &pages_[victm_fid];
  page_table_[new_pid] = victm_fid;
  victm_page->page_id_ = new_pid;
  victm_page->is_dirty_ = false;
  victm_page->pin_count_++;
  victm_page->ResetMemory();
  replacer_->Pin(victm_fid);
  *page_id = new_pid;
  LOG_INFO("Succed to allocate frame_id for page_id, page_id:%d, frame_id:%d", victm_page->page_id_, victm_fid);
  return victm_page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::unique_lock<std::mutex> lock(latch_);
  Page *fetch_page = nullptr;
  frame_id_t fetch_fid = -1;
  LOG_INFO("Starting to fetch page_id:%d", page_id);
  do {
    if (page_table_.find(page_id) != page_table_.end()) {
      fetch_fid = page_table_[page_id];
      fetch_page = &pages_[fetch_fid];
      fetch_page->pin_count_++;
      replacer_->Pin(fetch_fid);
      LOG_INFO("page_id:%d exits in page table, succeed to fetch ", page_id);
      return fetch_page;
    }
    LOG_INFO("page_id:%d don't exit in page table, start to allocate page", page_id);
    if (!free_list_.empty()) {
      fetch_fid = free_list_.front();
      free_list_.pop_front();
      LOG_INFO("Suceed allocate page for page_id:%d in free list", page_id);
      break;
    }
    bool is_all_spinned = true;
    for (int i = 0; i < static_cast<int>(pool_size_); i++) {
      if (pages_[i].GetPinCount() == 0) {
        is_all_spinned = false;
      }
    }
    if (is_all_spinned) {
      LOG_INFO("Failed to allocate frame for page_id:%d ,All page have been spinned", page_id);
      break;
    }
    if (!replacer_->Victim(&fetch_fid)) {
      LOG_INFO("Failed to victim frame for page_id:%d", page_id);
      break;
    }
    Page *replace_page = &pages_[fetch_fid];
    if (replace_page->IsDirty()) {
      disk_manager_->WritePage(replace_page->GetPageId(), replace_page->GetData());
    }
    page_table_.erase(replace_page->page_id_);
  } while (false);
  if (fetch_fid == -1) {
    return nullptr;
  }
  fetch_page = &pages_[fetch_fid];
  fetch_page->page_id_ = page_id;
  fetch_page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, fetch_page->GetData());
  page_table_[page_id] = fetch_fid;
  fetch_page->pin_count_++;
  replacer_->Pin(fetch_fid);
  LOG_INFO("Succeed to fetch page_id:%d", page_id);
  return fetch_page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::unique_lock<std::mutex> lock(latch_);
  LOG_INFO("Starting to delete page_id:%d", page_id);
  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_INFO("Page_id:%d don't exit", page_id);
    return true;
  }
  frame_id_t delete_fid = page_table_[page_id];
  Page *delete_page = &pages_[delete_fid];
  if (delete_page->GetPinCount() != 0) {
    LOG_INFO("Failed to delete page_id:%d, page has pin_count:%d", page_id, delete_page->GetPinCount());
    return false;
  }
  if (delete_page->IsDirty()) {
    LOG_INFO("Page_id:%d is dirty,need to write to disk", page_id);
    disk_manager_->WritePage(delete_page->GetPageId(), delete_page->GetData());
  }
  delete_page->page_id_ = INVALID_PAGE_ID;
  delete_page->is_dirty_ = false;
  delete_page->ResetMemory();
  free_list_.push_back(delete_fid);
  page_table_.erase(page_id);
  LOG_INFO("Succeed to delete page_id:%d", page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_INFO("Page_id:%d don't exit", page_id);
    return false;
  }
  frame_id_t unpin_fid = page_table_[page_id];
  Page *unpin_page = &pages_[unpin_fid];
  unpin_page->is_dirty_ = is_dirty;
  if (unpin_page->GetPinCount() <= 0) {
    return false;
  }
  if (--unpin_page->pin_count_ == 0) {
    replacer_->Unpin(unpin_fid);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
