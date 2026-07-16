#pragma once

#include <cstddef>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "storage/disk_manager.h"
#include "storage/page.h"

namespace quorumdb {

class BufferPoolManager {
 public:
  BufferPoolManager(std::size_t pool_size, DiskManager *disk_manager);

  BufferPoolManager(const BufferPoolManager &) = delete;
  BufferPoolManager &operator=(const BufferPoolManager &) = delete;

  Page *FetchPage(page_id_t page_id);
  Page *NewPage(page_id_t *out_page_id);
  bool UnpinPage(page_id_t page_id, bool is_dirty);
  bool FlushPage(page_id_t page_id);
  void FlushAllPages();
  bool DeletePage(page_id_t page_id);

 private:
  struct Frame {
    Page page;
    std::size_t pin_count{0};
    bool is_dirty{false};
  };

  std::size_t AcquireFrame();
  void AddToLru(std::size_t frame_index);
  void RemoveFromLru(std::size_t frame_index);
  void ResetFrame(std::size_t frame_index);

  DiskManager *disk_manager_;
  std::vector<Frame> frames_;
  std::unordered_map<page_id_t, std::size_t> page_table_;
  std::vector<std::size_t> free_frames_;
  std::list<std::size_t> lru_;
  std::vector<std::list<std::size_t>::iterator> lru_positions_;
  mutable std::mutex mutex_;
};

}  // namespace quorumdb
