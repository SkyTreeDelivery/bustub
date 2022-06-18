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
  std::scoped_lock lock{latch_};
  if(page_table_.count(page_id) == 0){
    return false;
  }
  Page* page = pages_ + page_table_.at(page_id);
  if(!page->IsDirty()){
    return true;
  }
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock lock{latch_};
  //也可以这样
  for(auto& v : page_table_){
    Page* page = pages_ + v.second;
    if(!page->IsDirty()){
      continue;
    }
    disk_manager_->WritePage(v.first, page->GetData());
    page->is_dirty_ = false;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  std::scoped_lock lock{latch_};
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // 如果freelist为空，则检查replacer中是否存在 pinnum == 0的page，如果都pinned，则返回nullptr
  if(free_list_.empty() && replacer_->Size() == 0){
    return nullptr;
  }
  Page * page;
  frame_id_t frame_id;
  // 如果freelist不为空，则取队头的frame_id，并new 一个page，并将该page放入到对应的frame槽
  // 如果freelist为空，则从replacer中拿到pinnum == 0 的page
  if(!free_list_.empty()){
    auto begin = free_list_.begin();
    frame_id = *begin;
    free_list_.erase(begin);
    page = pages_ + frame_id;
  } else {
    replacer_->Victim(&frame_id);
    page = pages_ + frame_id;
    // 保存dirty的数据
    if(page->IsDirty()){
      FlushPgImp(page->page_id_);
    }
    page_table_.erase(page->page_id_);
  }
  // 初始化page，调用AllocatePage分配pageid，并将page和对应的frame插入pageTable
  *page_id = AllocatePage();
  page->page_id_ =  *page_id;
  page->ResetMemory();
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_.insert(std::pair<page_id_t , frame_id_t>{page->page_id_, frame_id});
  replacer_->Pin(frame_id);
  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock lock{latch_};
  // 从pageTable中搜索page_id
  // 如果搜索到了，则说明此page在内存中，pincount计数后，可以直接返回。
  if(page_table_.count(page_id) == 1){
    frame_id_t frame_id = page_table_.at(page_id);
    Page * page = pages_ + frame_id;
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }
  // 如果没搜索到，说明需要加载到内存，如果freelist有空间，则直接使用，如果没有，则尝试从replacer中获取，如果没有空闲的pin，则返回nullptr。
  if(free_list_.empty() && replacer_->Size() == 0){
    return nullptr;
  }
  // 如果获取到了page，则从磁盘中读取page的数据，并更新到该page中。
  Page * page;
  frame_id_t frame_id;
  if(!free_list_.empty()){
    auto begin = free_list_.begin();
    frame_id = *begin;
    free_list_.erase(begin);
    page = pages_ + frame_id;
  } else {
    replacer_->Victim(&frame_id);
    page = pages_ + frame_id;
    // 保存dirty的数据
    if(page->IsDirty()){
      FlushPgImp(page->page_id_);
    }
    page_table_.erase(page->page_id_);
  }
  page->page_id_ =  page_id;
  disk_manager_->ReadPage(page_id, page->data_);
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page_table_.insert(std::pair<page_id_t , frame_id_t>{page->page_id_, frame_id});
  replacer_->Pin(frame_id);
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock{latch_};
  DeallocatePage(page_id);
  if(page_table_.count(page_id) == 0){
    return true;
  }
  frame_id_t frame_id = page_table_.at(page_id);
  Page * page = pages_ + frame_id;
  if(page->pin_count_ != 0){
    return false;
  }
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page_table_.erase(page->page_id_);
  free_list_.push_back(frame_id);
  replacer_->Pin(frame_id); // 从replacer中移除
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};
  // 如果该page_id不在内存中，则直接返回false
  if(page_table_.count(page_id) == 0){
    return false;
  }
  frame_id_t frame_id = page_table_.at(page_id);
  Page * page = pages_ + frame_id;
  if(page->pin_count_ == 0){
    return true;
  }
  if(is_dirty){
    page->is_dirty_ = is_dirty;
  }
  page->pin_count_--;
  if(page->GetPinCount() == 0){
    replacer_->Unpin(frame_id);
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
