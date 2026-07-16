#include "storage/disk_manager.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace quorumdb {

DiskManager::DiskManager(const std::string &filename) : filename_(filename) {
  std::lock_guard<std::mutex> lock(mutex_);
  OpenFile();

  const auto file_size = GetFileSizeUnlocked();
  next_page_id_ = static_cast<page_id_t>(file_size / PAGE_SIZE);
}

DiskManager::~DiskManager() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_.is_open()) {
    file_.close();
  }
}

void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (page_id < 0 || page_data == nullptr) {
    throw std::runtime_error("invalid page read request");
  }

  const auto offset = static_cast<std::streamoff>(page_id) * PAGE_SIZE;
  const auto file_size = GetFileSizeUnlocked();
  if (static_cast<std::size_t>(offset) >= file_size) {
    std::fill(page_data, page_data + PAGE_SIZE, '\0');
    return;
  }

  file_.clear();
  file_.seekg(offset);
  file_.read(page_data, PAGE_SIZE);
  const auto bytes_read = file_.gcount();
  if (bytes_read < static_cast<std::streamsize>(PAGE_SIZE)) {
    std::fill(page_data + bytes_read, page_data + PAGE_SIZE, '\0');
  }
  if (file_.bad()) {
    throw std::runtime_error("failed to read page");
  }
  file_.clear();
}

void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (page_id < 0 || page_data == nullptr) {
    throw std::runtime_error("invalid page write request");
  }

  const auto offset = static_cast<std::streamoff>(page_id) * PAGE_SIZE;
  file_.clear();
  file_.seekp(offset);
  file_.write(page_data, PAGE_SIZE);
  file_.flush();
  if (!file_) {
    throw std::runtime_error("failed to write page");
  }
  ++num_flushes_;
}

page_id_t DiskManager::AllocatePage() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!free_list_.empty()) {
    const page_id_t page_id = free_list_.back();
    free_list_.pop_back();
    return page_id;
  }
  if (next_page_id_ == std::numeric_limits<page_id_t>::max()) {
    throw std::runtime_error("page id limit reached");
  }
  return next_page_id_++;
}

void DiskManager::DeallocatePage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (page_id >= 0) {
    free_list_.push_back(page_id);
  }
}

std::size_t DiskManager::GetNumFlushes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return num_flushes_;
}

std::size_t DiskManager::GetFileSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return GetFileSizeUnlocked();
}

void DiskManager::OpenFile() {
  file_.open(filename_, std::ios::in | std::ios::out | std::ios::binary);
  if (file_.is_open()) {
    return;
  }

  std::ofstream create_file(filename_, std::ios::binary);
  if (!create_file) {
    throw std::runtime_error("failed to create backing file");
  }
  create_file.close();

  file_.clear();
  file_.open(filename_, std::ios::in | std::ios::out | std::ios::binary);
  if (!file_.is_open()) {
    throw std::runtime_error("failed to open backing file");
  }
}

std::size_t DiskManager::GetFileSizeUnlocked() const {
  file_.clear();
  file_.seekg(0, std::ios::end);
  const auto size = file_.tellg();
  if (size < 0) {
    throw std::runtime_error("failed to determine backing file size");
  }
  file_.clear();
  return static_cast<std::size_t>(size);
}

}  // namespace quorumdb
