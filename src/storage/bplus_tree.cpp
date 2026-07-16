#include "storage/bplus_tree.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace quorumdb {
namespace {

constexpr std::uint32_t kNodeMagic = 0x51444231U;

template <typename T>
void WriteValue(char *data, std::size_t *offset, const T &value) {
  std::memcpy(data + *offset, &value, sizeof(T));
  *offset += sizeof(T);
}

template <typename T>
T ReadValue(const char *data, std::size_t *offset) {
  T value{};
  std::memcpy(&value, data + *offset, sizeof(T));
  *offset += sizeof(T);
  return value;
}

void SerializeNode(const BPlusTreeNode &node, Page *page) {
  if (node.keys.size() > BPlusTreeNode::MAX_KEYS ||
      (node.is_leaf && node.values.size() != node.keys.size()) ||
      (!node.is_leaf && node.children.size() != node.keys.size() + 1)) {
    throw std::runtime_error("invalid B+ tree node");
  }

  page->ResetMemory();
  std::size_t offset = 0;
  const std::uint8_t is_leaf = node.is_leaf ? 1 : 0;
  const std::uint32_t key_count = static_cast<std::uint32_t>(node.keys.size());
  WriteValue(page->GetData(), &offset, kNodeMagic);
  WriteValue(page->GetData(), &offset, is_leaf);
  WriteValue(page->GetData(), &offset, node.page_id);
  WriteValue(page->GetData(), &offset, node.parent_id);
  WriteValue(page->GetData(), &offset, node.next_leaf);
  WriteValue(page->GetData(), &offset, key_count);
  for (key_t key : node.keys) {
    WriteValue(page->GetData(), &offset, key);
  }
  if (node.is_leaf) {
    for (value_t value : node.values) {
      WriteValue(page->GetData(), &offset, value);
    }
  } else {
    for (page_id_t child : node.children) {
      WriteValue(page->GetData(), &offset, child);
    }
  }
}

BPlusTreeNode DeserializeNode(const Page &page) {
  std::size_t offset = 0;
  const char *data = page.GetData();
  if (ReadValue<std::uint32_t>(data, &offset) != kNodeMagic) {
    throw std::runtime_error("invalid B+ tree page");
  }

  BPlusTreeNode node;
  node.is_leaf = ReadValue<std::uint8_t>(data, &offset) != 0;
  node.page_id = ReadValue<page_id_t>(data, &offset);
  node.parent_id = ReadValue<page_id_t>(data, &offset);
  node.next_leaf = ReadValue<page_id_t>(data, &offset);
  const auto key_count = ReadValue<std::uint32_t>(data, &offset);
  if (key_count > BPlusTreeNode::MAX_KEYS) {
    throw std::runtime_error("invalid B+ tree key count");
  }
  node.keys.reserve(key_count);
  for (std::uint32_t index = 0; index < key_count; ++index) {
    node.keys.push_back(ReadValue<key_t>(data, &offset));
  }
  if (node.is_leaf) {
    node.values.reserve(key_count);
    for (std::uint32_t index = 0; index < key_count; ++index) {
      node.values.push_back(ReadValue<value_t>(data, &offset));
    }
  } else {
    node.children.reserve(key_count + 1);
    for (std::uint32_t index = 0; index <= key_count; ++index) {
      node.children.push_back(ReadValue<page_id_t>(data, &offset));
    }
  }
  return node;
}

}  // namespace

BPlusTree::BPlusTree(BufferPoolManager *buffer_pool_manager)
    : buffer_pool_manager_(buffer_pool_manager) {
  if (buffer_pool_manager_ == nullptr) {
    throw std::invalid_argument("buffer pool manager must not be null");
  }
}

bool BPlusTree::Insert(key_t key, value_t value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (root_page_id_ == INVALID_PAGE_ID) {
    BPlusTreeNode root;
    root.keys.push_back(key);
    root.values.push_back(value);
    root_page_id_ = CreateNode(root);
    return true;
  }

  BPlusTreeNode leaf = ReadNode(FindLeaf(key));
  const auto position = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
  if (position != leaf.keys.end() && *position == key) {
    return false;
  }
  const auto index = static_cast<std::size_t>(position - leaf.keys.begin());
  leaf.keys.insert(position, key);
  leaf.values.insert(leaf.values.begin() + static_cast<std::ptrdiff_t>(index), value);
  if (leaf.keys.size() <= BPlusTreeNode::MAX_KEYS) {
    WriteNode(leaf);
    if (index == 0) {
      PropagateMinimumChange(leaf.page_id);
    }
    return true;
  }

  BPlusTreeNode right;
  right.is_leaf = true;
  right.parent_id = leaf.parent_id;
  const std::size_t split_index = (leaf.keys.size() + 1) / 2;
  right.keys.assign(leaf.keys.begin() + static_cast<std::ptrdiff_t>(split_index), leaf.keys.end());
  right.values.assign(leaf.values.begin() + static_cast<std::ptrdiff_t>(split_index), leaf.values.end());
  leaf.keys.resize(split_index);
  leaf.values.resize(split_index);
  right.next_leaf = leaf.next_leaf;
  right.page_id = CreateNode(right);
  leaf.next_leaf = right.page_id;
  WriteNode(leaf);
  InsertIntoParent(leaf, right.keys.front(), right);
  return true;
}

bool BPlusTree::Search(key_t key, value_t *out_value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  const BPlusTreeNode leaf = ReadNode(FindLeaf(key));
  const auto position = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
  if (position == leaf.keys.end() || *position != key) {
    return false;
  }
  if (out_value != nullptr) {
    *out_value = leaf.values[static_cast<std::size_t>(position - leaf.keys.begin())];
  }
  return true;
}

bool BPlusTree::Delete(key_t key) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  BPlusTreeNode leaf = ReadNode(FindLeaf(key));
  const auto position = std::lower_bound(leaf.keys.begin(), leaf.keys.end(), key);
  if (position == leaf.keys.end() || *position != key) {
    return false;
  }
  const std::size_t index = static_cast<std::size_t>(position - leaf.keys.begin());
  leaf.keys.erase(position);
  leaf.values.erase(leaf.values.begin() + static_cast<std::ptrdiff_t>(index));
  if (leaf.page_id == root_page_id_ || leaf.keys.size() >= BPlusTreeNode::MAX_KEYS / 2) {
    WriteNode(leaf);
    if (!leaf.keys.empty() && index == 0) {
      PropagateMinimumChange(leaf.page_id);
    }
    return true;
  }

  BPlusTreeNode parent = ReadNode(leaf.parent_id);
  const auto child_position = std::find(parent.children.begin(), parent.children.end(), leaf.page_id);
  const std::size_t child_index = static_cast<std::size_t>(child_position - parent.children.begin());
  if (child_index > 0) {
    BPlusTreeNode left = ReadNode(parent.children[child_index - 1]);
    if (left.keys.size() > BPlusTreeNode::MAX_KEYS / 2) {
      leaf.keys.insert(leaf.keys.begin(), left.keys.back());
      leaf.values.insert(leaf.values.begin(), left.values.back());
      left.keys.pop_back();
      left.values.pop_back();
      parent.keys[child_index - 1] = leaf.keys.front();
      WriteNode(left);
      WriteNode(leaf);
      WriteNode(parent);
      return true;
    }
  }
  if (child_index + 1 < parent.children.size()) {
    BPlusTreeNode right = ReadNode(parent.children[child_index + 1]);
    if (right.keys.size() > BPlusTreeNode::MAX_KEYS / 2) {
      leaf.keys.push_back(right.keys.front());
      leaf.values.push_back(right.values.front());
      right.keys.erase(right.keys.begin());
      right.values.erase(right.values.begin());
      parent.keys[child_index] = right.keys.front();
      WriteNode(leaf);
      WriteNode(right);
      WriteNode(parent);
      return true;
    }
  }

  if (child_index > 0) {
    BPlusTreeNode left = ReadNode(parent.children[child_index - 1]);
    left.keys.insert(left.keys.end(), leaf.keys.begin(), leaf.keys.end());
    left.values.insert(left.values.end(), leaf.values.begin(), leaf.values.end());
    left.next_leaf = leaf.next_leaf;
    parent.keys.erase(parent.keys.begin() + static_cast<std::ptrdiff_t>(child_index - 1));
    parent.children.erase(parent.children.begin() + static_cast<std::ptrdiff_t>(child_index));
    WriteNode(left);
    WriteNode(parent);
    buffer_pool_manager_->DeletePage(leaf.page_id);
  } else {
    BPlusTreeNode right = ReadNode(parent.children[child_index + 1]);
    leaf.keys.insert(leaf.keys.end(), right.keys.begin(), right.keys.end());
    leaf.values.insert(leaf.values.end(), right.values.begin(), right.values.end());
    leaf.next_leaf = right.next_leaf;
    parent.keys.erase(parent.keys.begin() + static_cast<std::ptrdiff_t>(child_index));
    parent.children.erase(parent.children.begin() + static_cast<std::ptrdiff_t>(child_index + 1));
    WriteNode(leaf);
    WriteNode(parent);
    buffer_pool_manager_->DeletePage(right.page_id);
  }
  HandleInternalUnderflow(parent.page_id);
  return true;
}

void BPlusTree::RangeScan(key_t start, key_t end,
                          std::vector<std::pair<key_t, value_t>> *out) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (out == nullptr || root_page_id_ == INVALID_PAGE_ID || start > end) {
    return;
  }
  page_id_t leaf_id = FindLeaf(start);
  while (leaf_id != INVALID_PAGE_ID) {
    const BPlusTreeNode leaf = ReadNode(leaf_id);
    for (std::size_t index = 0; index < leaf.keys.size(); ++index) {
      if (leaf.keys[index] < start) {
        continue;
      }
      if (leaf.keys[index] > end) {
        return;
      }
      out->emplace_back(leaf.keys[index], leaf.values[index]);
    }
    leaf_id = leaf.next_leaf;
  }
}

BPlusTreeNode BPlusTree::ReadNode(page_id_t page_id) {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("buffer pool has no evictable frame");
  }
  BPlusTreeNode node = DeserializeNode(*page);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return node;
}

void BPlusTree::WriteNode(const BPlusTreeNode &node) {
  Page *page = buffer_pool_manager_->FetchPage(node.page_id);
  if (page == nullptr) {
    throw std::runtime_error("buffer pool has no evictable frame");
  }
  SerializeNode(node, page);
  buffer_pool_manager_->UnpinPage(node.page_id, true);
}

page_id_t BPlusTree::CreateNode(const BPlusTreeNode &node) {
  page_id_t page_id = INVALID_PAGE_ID;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw std::runtime_error("buffer pool has no free frame");
  }
  BPlusTreeNode initialized = node;
  initialized.page_id = page_id;
  SerializeNode(initialized, page);
  buffer_pool_manager_->UnpinPage(page_id, true);
  return page_id;
}

page_id_t BPlusTree::FindLeaf(key_t key) {
  page_id_t current = root_page_id_;
  while (current != INVALID_PAGE_ID) {
    const BPlusTreeNode node = ReadNode(current);
    if (node.is_leaf) {
      return current;
    }
    const auto child_index = static_cast<std::size_t>(
        std::upper_bound(node.keys.begin(), node.keys.end(), key) - node.keys.begin());
    current = node.children[child_index];
  }
  return INVALID_PAGE_ID;
}

void BPlusTree::InsertIntoParent(BPlusTreeNode left, key_t separator, BPlusTreeNode right) {
  if (left.parent_id == INVALID_PAGE_ID) {
    BPlusTreeNode root;
    root.is_leaf = false;
    root.keys.push_back(separator);
    root.children = {left.page_id, right.page_id};
    root_page_id_ = CreateNode(root);
    left.parent_id = root_page_id_;
    right.parent_id = root_page_id_;
    WriteNode(left);
    WriteNode(right);
    return;
  }

  BPlusTreeNode parent = ReadNode(left.parent_id);
  const auto position = std::find(parent.children.begin(), parent.children.end(), left.page_id);
  const std::size_t child_index = static_cast<std::size_t>(position - parent.children.begin());
  parent.keys.insert(parent.keys.begin() + static_cast<std::ptrdiff_t>(child_index), separator);
  parent.children.insert(parent.children.begin() + static_cast<std::ptrdiff_t>(child_index + 1),
                         right.page_id);
  right.parent_id = parent.page_id;
  WriteNode(right);
  if (parent.keys.size() <= BPlusTreeNode::MAX_KEYS) {
    WriteNode(parent);
    return;
  }

  BPlusTreeNode new_right;
  new_right.is_leaf = false;
  new_right.parent_id = parent.parent_id;
  const std::size_t middle = parent.keys.size() / 2;
  const key_t promoted_key = parent.keys[middle];
  new_right.keys.assign(parent.keys.begin() + static_cast<std::ptrdiff_t>(middle + 1), parent.keys.end());
  new_right.children.assign(parent.children.begin() + static_cast<std::ptrdiff_t>(middle + 1),
                            parent.children.end());
  parent.keys.resize(middle);
  parent.children.resize(middle + 1);
  new_right.page_id = CreateNode(new_right);
  SetChildrenParent(new_right.children, new_right.page_id);
  WriteNode(parent);
  InsertIntoParent(parent, promoted_key, new_right);
}

void BPlusTree::HandleInternalUnderflow(page_id_t page_id) {
  BPlusTreeNode node = ReadNode(page_id);
  if (node.page_id == root_page_id_) {
    if (node.keys.empty() && !node.children.empty()) {
      root_page_id_ = node.children.front();
      BPlusTreeNode child = ReadNode(root_page_id_);
      child.parent_id = INVALID_PAGE_ID;
      WriteNode(child);
      buffer_pool_manager_->DeletePage(node.page_id);
    }
    return;
  }
  if (node.keys.size() >= BPlusTreeNode::MAX_KEYS / 2) {
    return;
  }

  BPlusTreeNode parent = ReadNode(node.parent_id);
  const auto position = std::find(parent.children.begin(), parent.children.end(), node.page_id);
  const std::size_t child_index = static_cast<std::size_t>(position - parent.children.begin());
  if (child_index > 0) {
    BPlusTreeNode left = ReadNode(parent.children[child_index - 1]);
    if (left.keys.size() > BPlusTreeNode::MAX_KEYS / 2) {
      node.keys.insert(node.keys.begin(), parent.keys[child_index - 1]);
      node.children.insert(node.children.begin(), left.children.back());
      parent.keys[child_index - 1] = left.keys.back();
      left.keys.pop_back();
      left.children.pop_back();
      SetChildrenParent({node.children.front()}, node.page_id);
      WriteNode(left);
      WriteNode(node);
      WriteNode(parent);
      return;
    }
  }
  if (child_index + 1 < parent.children.size()) {
    BPlusTreeNode right = ReadNode(parent.children[child_index + 1]);
    if (right.keys.size() > BPlusTreeNode::MAX_KEYS / 2) {
      node.keys.push_back(parent.keys[child_index]);
      node.children.push_back(right.children.front());
      parent.keys[child_index] = right.keys.front();
      right.keys.erase(right.keys.begin());
      right.children.erase(right.children.begin());
      SetChildrenParent({node.children.back()}, node.page_id);
      WriteNode(node);
      WriteNode(right);
      WriteNode(parent);
      return;
    }
  }

  if (child_index > 0) {
    BPlusTreeNode left = ReadNode(parent.children[child_index - 1]);
    left.keys.push_back(parent.keys[child_index - 1]);
    left.keys.insert(left.keys.end(), node.keys.begin(), node.keys.end());
    left.children.insert(left.children.end(), node.children.begin(), node.children.end());
    parent.keys.erase(parent.keys.begin() + static_cast<std::ptrdiff_t>(child_index - 1));
    parent.children.erase(parent.children.begin() + static_cast<std::ptrdiff_t>(child_index));
    SetChildrenParent(node.children, left.page_id);
    WriteNode(left);
    WriteNode(parent);
    buffer_pool_manager_->DeletePage(node.page_id);
  } else {
    BPlusTreeNode right = ReadNode(parent.children[child_index + 1]);
    node.keys.push_back(parent.keys[child_index]);
    node.keys.insert(node.keys.end(), right.keys.begin(), right.keys.end());
    node.children.insert(node.children.end(), right.children.begin(), right.children.end());
    parent.keys.erase(parent.keys.begin() + static_cast<std::ptrdiff_t>(child_index));
    parent.children.erase(parent.children.begin() + static_cast<std::ptrdiff_t>(child_index + 1));
    SetChildrenParent(right.children, node.page_id);
    WriteNode(node);
    WriteNode(parent);
    buffer_pool_manager_->DeletePage(right.page_id);
  }
  HandleInternalUnderflow(parent.page_id);
}

void BPlusTree::PropagateMinimumChange(page_id_t page_id) {
  page_id_t current = page_id;
  while (current != root_page_id_) {
    const BPlusTreeNode node = ReadNode(current);
    BPlusTreeNode parent = ReadNode(node.parent_id);
    const auto position = std::find(parent.children.begin(), parent.children.end(), current);
    const std::size_t child_index = static_cast<std::size_t>(position - parent.children.begin());
    if (child_index > 0) {
      parent.keys[child_index - 1] = FirstKey(current);
      WriteNode(parent);
      return;
    }
    current = parent.page_id;
  }
}

key_t BPlusTree::FirstKey(page_id_t page_id) {
  BPlusTreeNode node = ReadNode(page_id);
  while (!node.is_leaf) {
    node = ReadNode(node.children.front());
  }
  return node.keys.front();
}

void BPlusTree::SetChildrenParent(const std::vector<page_id_t> &children, page_id_t parent_id) {
  for (page_id_t child_id : children) {
    BPlusTreeNode child = ReadNode(child_id);
    child.parent_id = parent_id;
    WriteNode(child);
  }
}

}  // namespace quorumdb
