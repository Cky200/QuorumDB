#pragma once

#include <mutex>
#include <utility>
#include <vector>

#include "storage/bplus_tree_node.h"
#include "storage/buffer_pool_manager.h"

namespace quorumdb {

class BPlusTree {
 public:
  explicit BPlusTree(BufferPoolManager *buffer_pool_manager);

  BPlusTree(const BPlusTree &) = delete;
  BPlusTree &operator=(const BPlusTree &) = delete;

  bool Insert(key_t key, value_t value);
  bool Search(key_t key, value_t *out_value);
  bool Delete(key_t key);
  void RangeScan(key_t start, key_t end, std::vector<std::pair<key_t, value_t>> *out);

 private:
  BPlusTreeNode ReadNode(page_id_t page_id);
  void WriteNode(const BPlusTreeNode &node);
  page_id_t CreateNode(const BPlusTreeNode &node);
  page_id_t FindLeaf(key_t key);
  void InsertIntoParent(BPlusTreeNode left, key_t separator, BPlusTreeNode right);
  void HandleInternalUnderflow(page_id_t page_id);
  void PropagateMinimumChange(page_id_t page_id);
  key_t FirstKey(page_id_t page_id);
  void SetChildrenParent(const std::vector<page_id_t> &children, page_id_t parent_id);

  BufferPoolManager *buffer_pool_manager_;
  page_id_t root_page_id_{INVALID_PAGE_ID};
  mutable std::mutex mutex_;
};

}  // namespace quorumdb
