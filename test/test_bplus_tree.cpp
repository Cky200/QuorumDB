#include <algorithm>
#include <cassert>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include "storage/bplus_tree.h"

namespace {

std::string TestFilename(const char *name) {
  return std::string("test_bplus_tree_") + name + ".db";
}

void RemoveFile(const std::string &filename) { std::remove(filename.c_str()); }

void TestOrderedSearchAndRangeScan() {
  const auto filename = TestFilename("ordered");
  RemoveFile(filename);
  {
    quorumdb::DiskManager disk_manager(filename);
    quorumdb::BufferPoolManager buffer_pool(8, &disk_manager);
    quorumdb::BPlusTree tree(&buffer_pool);
    for (quorumdb::key_t key : {9, 1, 7, 3, 5, 2, 8, 4, 6}) {
      assert(tree.Insert(key, key * 10));
    }
    for (quorumdb::key_t key = 1; key <= 9; ++key) {
      quorumdb::value_t value = 0;
      assert(tree.Search(key, &value));
      assert(value == key * 10);
    }
    std::vector<std::pair<quorumdb::key_t, quorumdb::value_t>> result;
    tree.RangeScan(2, 8, &result);
    assert(result.size() == 7);
    for (std::size_t index = 0; index < result.size(); ++index) {
      assert(result[index].first == static_cast<quorumdb::key_t>(index + 2));
      assert(result[index].second == static_cast<quorumdb::value_t>((index + 2) * 10));
    }
  }
  RemoveFile(filename);
}

void TestMultiLevelSplits() {
  const auto filename = TestFilename("splits");
  RemoveFile(filename);
  {
    quorumdb::DiskManager disk_manager(filename);
    quorumdb::BufferPoolManager buffer_pool(8, &disk_manager);
    quorumdb::BPlusTree tree(&buffer_pool);
    for (quorumdb::key_t key = 0; key < 40; ++key) {
      assert(tree.Insert(key, key + 100));
    }
    for (quorumdb::key_t key = 0; key < 40; ++key) {
      quorumdb::value_t value = 0;
      assert(tree.Search(key, &value));
      assert(value == key + 100);
    }
  }
  RemoveFile(filename);
}

void TestDeleteRebalancesAndKeepsOrder() {
  const auto filename = TestFilename("delete");
  RemoveFile(filename);
  {
    quorumdb::DiskManager disk_manager(filename);
    quorumdb::BufferPoolManager buffer_pool(8, &disk_manager);
    quorumdb::BPlusTree tree(&buffer_pool);
    for (quorumdb::key_t key = 0; key < 20; ++key) {
      assert(tree.Insert(key, key));
    }
    for (quorumdb::key_t key = 0; key < 15; ++key) {
      assert(tree.Delete(key));
    }
    for (quorumdb::key_t key = 0; key < 15; ++key) {
      assert(!tree.Search(key, nullptr));
    }
    std::vector<std::pair<quorumdb::key_t, quorumdb::value_t>> result;
    tree.RangeScan(0, 100, &result);
    assert(result.size() == 5);
    for (std::size_t index = 0; index < result.size(); ++index) {
      assert(result[index].first == static_cast<quorumdb::key_t>(index + 15));
      assert(result[index].second == result[index].first);
      assert(tree.Search(result[index].first, nullptr));
    }
  }
  RemoveFile(filename);
}

void TestDuplicateAndMissingKey() {
  const auto filename = TestFilename("duplicate");
  RemoveFile(filename);
  {
    quorumdb::DiskManager disk_manager(filename);
    quorumdb::BufferPoolManager buffer_pool(4, &disk_manager);
    quorumdb::BPlusTree tree(&buffer_pool);
    assert(tree.Insert(42, 100));
    assert(!tree.Insert(42, 200));
    quorumdb::value_t value = 0;
    assert(tree.Search(42, &value));
    assert(value == 100);
    assert(!tree.Search(99, nullptr));
  }
  RemoveFile(filename);
}

}  // namespace

int main() {
  TestOrderedSearchAndRangeScan();
  TestMultiLevelSplits();
  TestDeleteRebalancesAndKeepsOrder();
  TestDuplicateAndMissingKey();
  return 0;
}
