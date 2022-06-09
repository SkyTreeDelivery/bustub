//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { this->size = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (this->Size() == 0) {
    return false;
  }
  auto iter = list_t_.begin();
  *frame_id = *iter;
  list_t_.erase(iter);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock{mutex_};
  auto iter = std::find(list_t_.begin(), list_t_.end(), frame_id);
  if (iter == list_t_.end()) {
    return;
  }
  list_t_.erase(iter);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock{mutex_};
  // 如果已经添加过，则返回
  if (std::count(list_t_.begin(), list_t_.end(), frame_id) != 0) {
    return;
  }
  // 如果超出容器容量，则返回
  if (list_t_.size() >= this->size) {
    return;
  }
  list_t_.push_back(frame_id);
}

size_t LRUReplacer::Size() {
  std::scoped_lock lock{mutex_};
  return list_t_.size();
  ;
}

}  // namespace bustub
