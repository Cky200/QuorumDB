#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "storage/page.h"

namespace quorumdb {

using key_t = std::int64_t;
using value_t = std::int64_t;

struct BPlusTreeNode {
  static constexpr std::size_t MAX_KEYS = 4;

  bool is_leaf{true};
  page_id_t page_id{INVALID_PAGE_ID};
  page_id_t parent_id{INVALID_PAGE_ID};
  std::vector<key_t> keys;
  std::vector<value_t> values;
  std::vector<page_id_t> children;
  page_id_t next_leaf{INVALID_PAGE_ID};
};

}  // namespace quorumdb
