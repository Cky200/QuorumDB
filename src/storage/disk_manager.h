#pragma once

#include <cstddef>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

#include "storage/page.h"

namespace quorumdb {

class DiskManager {
 public:
  explicit DiskManager(const std::string &filename);
  ~DiskManager();

  DiskManager(const DiskManager &) = delete;
  DiskManager &operator=(const DiskManager &) = delete;

  void ReadPage(page_id_t page_id, char *page_data);
  void WritePage(page_id_t page_id, const char *page_data);

  page_id_t AllocatePage();
  void DeallocatePage(page_id_t page_id);

  std::size_t GetNumFlushes() const;
  std::size_t GetFileSize() const;

 private:
  void OpenFile();
  std::size_t GetFileSizeUnlocked() const;

  std::string filename_;
  mutable std::fstream file_;
  page_id_t next_page_id_{0};
  std::vector<page_id_t> free_list_;
  std::size_t num_flushes_{0};
  mutable std::mutex mutex_;
};

}  // namespace quorumdb
