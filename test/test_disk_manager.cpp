#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

#include "storage/disk_manager.h"
#include "storage/page.h"

namespace {

std::string TestFilename(const char *name) {
  return std::string("test_") + name + ".db";
}

void RemoveFile(const std::string &filename) { std::remove(filename.c_str()); }

void TestWriteThenRead() {
  const auto filename = TestFilename("write_read");
  RemoveFile(filename);

  {
    quorumdb::DiskManager disk_manager(filename);
    const auto page_id = disk_manager.AllocatePage();
    quorumdb::Page written;
    quorumdb::Page read;
    std::fill(written.GetData(), written.GetData() + quorumdb::PAGE_SIZE, 'x');

    disk_manager.WritePage(page_id, written.GetData());
    disk_manager.ReadPage(page_id, read.GetData());
    assert(std::memcmp(written.GetData(), read.GetData(), quorumdb::PAGE_SIZE) == 0);
  }
  RemoveFile(filename);
}

void TestUnwrittenPageReadsAsZero() {
  const auto filename = TestFilename("unwritten");
  RemoveFile(filename);

  {
    quorumdb::DiskManager disk_manager(filename);
    const auto page_id = disk_manager.AllocatePage();
    quorumdb::Page page;
    std::fill(page.GetData(), page.GetData() + quorumdb::PAGE_SIZE, 'x');

    disk_manager.ReadPage(page_id, page.GetData());
    assert(std::all_of(page.GetData(), page.GetData() + quorumdb::PAGE_SIZE,
                       [](char byte) { return byte == '\0'; }));
  }
  RemoveFile(filename);
}

void TestAllocationIdsAreUnique() {
  const auto filename = TestFilename("allocation");
  RemoveFile(filename);

  {
    quorumdb::DiskManager disk_manager(filename);
    const auto first = disk_manager.AllocatePage();
    const auto second = disk_manager.AllocatePage();
    const auto third = disk_manager.AllocatePage();
    assert(first != second && second != third && first != third);
  }
  RemoveFile(filename);
}

void TestDataPersistsAfterReopen() {
  const auto filename = TestFilename("persistence");
  RemoveFile(filename);
  constexpr char kMessage[] = "persistent data";

  {
    quorumdb::DiskManager disk_manager(filename);
    const auto page_id = disk_manager.AllocatePage();
    quorumdb::Page page;
    std::memcpy(page.GetData(), kMessage, sizeof(kMessage));
    disk_manager.WritePage(page_id, page.GetData());
  }

  {
    quorumdb::DiskManager disk_manager(filename);
    quorumdb::Page page;
    disk_manager.ReadPage(0, page.GetData());
    assert(std::strcmp(page.GetData(), kMessage) == 0);
  }
  RemoveFile(filename);
}

}  // namespace

int main() {
  TestWriteThenRead();
  TestUnwrittenPageReadsAsZero();
  TestAllocationIdsAreUnique();
  TestDataPersistsAfterReopen();
  return 0;
}
