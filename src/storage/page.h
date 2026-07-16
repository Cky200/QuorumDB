#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace quorumdb {

using page_id_t = std::int32_t;

constexpr std::size_t PAGE_SIZE = 4096;
constexpr page_id_t INVALID_PAGE_ID = -1;

class Page {
 public:
  Page() : page_id_(INVALID_PAGE_ID) { ResetMemory(); }

  char *GetData() { return data_; }
  const char *GetData() const { return data_; }

  page_id_t GetPageId() const { return page_id_; }
  void SetPageId(page_id_t page_id) { page_id_ = page_id; }

  void ResetMemory() { std::fill(data_, data_ + PAGE_SIZE, '\0'); }

 private:
  char data_[PAGE_SIZE];
  page_id_t page_id_;
};

}  // namespace quorumdb
