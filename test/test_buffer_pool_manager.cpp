#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

#include "storage/buffer_pool_manager.h"

namespace {

std::string TestFilename(const char *name) {
  return std::string("test_buffer_pool_") + name + ".db";
}

void RemoveFile(const std::string &filename) { std::remove(filename.c_str()); }

void TestFetchReturnsSamePage() {
  const auto filename = TestFilename("same_page");
  RemoveFile(filename);
  {
    quorumdb::DiskManager disk_manager(filename);
    quorumdb::BufferPoolManager buffer_pool(2, &disk_manager);
    const auto page_id = disk_manager.AllocatePage();
    quorumdb::Page *first = buffer_pool.FetchPage(page_id);
    quorumdb::Page *second = buffer_pool.FetchPage(page_id);
    assert(first != nullptr);
    assert(first == second);
    assert(buffer_pool.UnpinPage(page_id, false));
    assert(buffer_pool.UnpinPage(page_id, false));
  }
  RemoveFile(filename);
}

void TestEvictionFlushesDirtyPage() {
  const auto filename = TestFilename("eviction");
  RemoveFile(filename);
  {
    quorumdb::DiskManager disk_manager(filename);
    quorumdb::BufferPoolManager buffer_pool(1, &disk_manager);
    quorumdb::page_id_t first_page_id;
    quorumdb::Page *first = buffer_pool.NewPage(&first_page_id);
    assert(first != nullptr);
    constexpr char kMessage[] = "flush on eviction";
    std::memcpy(first->GetData(), kMessage, sizeof(kMessage));
    assert(buffer_pool.UnpinPage(first_page_id, true));

    const auto second_page_id = disk_manager.AllocatePage();
    quorumdb::Page *second = buffer_pool.FetchPage(second_page_id);
    assert(second != nullptr);
    assert(buffer_pool.UnpinPage(second_page_id, false));

    quorumdb::Page *reloaded = buffer_pool.FetchPage(first_page_id);
    assert(reloaded != nullptr);
    assert(std::strcmp(reloaded->GetData(), kMessage) == 0);
    assert(buffer_pool.UnpinPage(first_page_id, false));
  }
  RemoveFile(filename);
}

void TestPinnedPageIsNotEvicted() {
  const auto filename = TestFilename("pinned");
  RemoveFile(filename);
  {
    quorumdb::DiskManager disk_manager(filename);
    quorumdb::BufferPoolManager buffer_pool(1, &disk_manager);
    quorumdb::page_id_t pinned_page_id;
    quorumdb::Page *pinned = buffer_pool.NewPage(&pinned_page_id);
    assert(pinned != nullptr);

    const auto other_page_id = disk_manager.AllocatePage();
    assert(buffer_pool.FetchPage(other_page_id) == nullptr);
    assert(buffer_pool.FetchPage(pinned_page_id) == pinned);
    assert(buffer_pool.UnpinPage(pinned_page_id, false));
    assert(buffer_pool.UnpinPage(pinned_page_id, false));
  }
  RemoveFile(filename);
}

void TestFlushAllPagesPersistsDirtyData() {
  const auto filename = TestFilename("flush_all");
  RemoveFile(filename);
  {
    quorumdb::DiskManager disk_manager(filename);
    quorumdb::BufferPoolManager buffer_pool(2, &disk_manager);
    quorumdb::page_id_t first_page_id;
    quorumdb::page_id_t second_page_id;
    quorumdb::Page *first = buffer_pool.NewPage(&first_page_id);
    quorumdb::Page *second = buffer_pool.NewPage(&second_page_id);
    assert(first != nullptr && second != nullptr);
    std::strcpy(first->GetData(), "first page");
    std::strcpy(second->GetData(), "second page");
    assert(buffer_pool.UnpinPage(first_page_id, true));
    assert(buffer_pool.UnpinPage(second_page_id, true));

    buffer_pool.FlushAllPages();
    quorumdb::Page first_read;
    quorumdb::Page second_read;
    disk_manager.ReadPage(first_page_id, first_read.GetData());
    disk_manager.ReadPage(second_page_id, second_read.GetData());
    assert(std::strcmp(first_read.GetData(), "first page") == 0);
    assert(std::strcmp(second_read.GetData(), "second page") == 0);
  }
  RemoveFile(filename);
}

}  // namespace

int main() {
  TestFetchReturnsSamePage();
  TestEvictionFlushesDirtyPage();
  TestPinnedPageIsNotEvicted();
  TestFlushAllPagesPersistsDirtyData();
  return 0;
}
