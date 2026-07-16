#include "storage/buffer_pool_manager.h"

#include <stdexcept>

namespace quorumdb {

BufferPoolManager::BufferPoolManager(std::size_t pool_size, DiskManager *disk_manager)
    : disk_manager_(disk_manager), frames_(pool_size), lru_positions_(pool_size, lru_.end()) {
  if (disk_manager_ == nullptr) {
    throw std::invalid_argument("disk manager must not be null");
  }
  for (std::size_t index = pool_size; index > 0; --index) {
    free_frames_.push_back(index - 1);
  }
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  const auto existing = page_table_.find(page_id);
  if (existing != page_table_.end()) {
    Frame &frame = frames_[existing->second];
    if (frame.pin_count == 0) {
      RemoveFromLru(existing->second);
    }
    ++frame.pin_count;
    return &frame.page;
  }

  const std::size_t frame_index = AcquireFrame();
  if (frame_index == frames_.size()) {
    return nullptr;
  }

  Frame &frame = frames_[frame_index];
  disk_manager_->ReadPage(page_id, frame.page.GetData());
  frame.page.SetPageId(page_id);
  frame.pin_count = 1;
  frame.is_dirty = false;
  page_table_.emplace(page_id, frame_index);
  return &frame.page;
}

Page *BufferPoolManager::NewPage(page_id_t *out_page_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (out_page_id == nullptr) {
    return nullptr;
  }

  const std::size_t frame_index = AcquireFrame();
  if (frame_index == frames_.size()) {
    return nullptr;
  }

  const page_id_t page_id = disk_manager_->AllocatePage();
  Frame &frame = frames_[frame_index];
  frame.page.ResetMemory();
  frame.page.SetPageId(page_id);
  frame.pin_count = 1;
  frame.is_dirty = false;
  page_table_.emplace(page_id, frame_index);
  *out_page_id = page_id;
  return &frame.page;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto entry = page_table_.find(page_id);
  if (entry == page_table_.end()) {
    return false;
  }

  Frame &frame = frames_[entry->second];
  if (frame.pin_count == 0) {
    return false;
  }
  --frame.pin_count;
  frame.is_dirty = frame.is_dirty || is_dirty;
  if (frame.pin_count == 0) {
    AddToLru(entry->second);
  }
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto entry = page_table_.find(page_id);
  if (entry == page_table_.end()) {
    return false;
  }

  Frame &frame = frames_[entry->second];
  if (frame.is_dirty) {
    disk_manager_->WritePage(page_id, frame.page.GetData());
    frame.is_dirty = false;
  }
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto &entry : page_table_) {
    Frame &frame = frames_[entry.second];
    if (frame.is_dirty) {
      disk_manager_->WritePage(entry.first, frame.page.GetData());
      frame.is_dirty = false;
    }
  }
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto entry = page_table_.find(page_id);
  if (entry != page_table_.end()) {
    Frame &frame = frames_[entry->second];
    if (frame.pin_count != 0) {
      return false;
    }
    RemoveFromLru(entry->second);
    ResetFrame(entry->second);
    free_frames_.push_back(entry->second);
    page_table_.erase(entry);
  }
  disk_manager_->DeallocatePage(page_id);
  return true;
}

std::size_t BufferPoolManager::AcquireFrame() {
  if (!free_frames_.empty()) {
    const std::size_t frame_index = free_frames_.back();
    free_frames_.pop_back();
    return frame_index;
  }
  if (lru_.empty()) {
    return frames_.size();
  }

  const std::size_t frame_index = lru_.back();
  RemoveFromLru(frame_index);
  Frame &frame = frames_[frame_index];
  const page_id_t old_page_id = frame.page.GetPageId();
  if (frame.is_dirty) {
    disk_manager_->WritePage(old_page_id, frame.page.GetData());
  }
  page_table_.erase(old_page_id);
  ResetFrame(frame_index);
  return frame_index;
}

void BufferPoolManager::AddToLru(std::size_t frame_index) {
  if (lru_positions_[frame_index] != lru_.end()) {
    return;
  }
  lru_.push_front(frame_index);
  lru_positions_[frame_index] = lru_.begin();
}

void BufferPoolManager::RemoveFromLru(std::size_t frame_index) {
  if (lru_positions_[frame_index] == lru_.end()) {
    return;
  }
  lru_.erase(lru_positions_[frame_index]);
  lru_positions_[frame_index] = lru_.end();
}

void BufferPoolManager::ResetFrame(std::size_t frame_index) {
  Frame &frame = frames_[frame_index];
  frame.page.ResetMemory();
  frame.page.SetPageId(INVALID_PAGE_ID);
  frame.pin_count = 0;
  frame.is_dirty = false;
}

}  // namespace quorumdb
