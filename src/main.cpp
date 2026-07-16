#include <cstring>
#include <iostream>

#include "storage/disk_manager.h"
#include "storage/page.h"

int main() {
  quorumdb::DiskManager disk_manager("quorumdb.db");
  quorumdb::Page page;
  page.SetPageId(disk_manager.AllocatePage());

  const char message[] = "Hello from QuorumDB";
  std::memcpy(page.GetData(), message, sizeof(message));
  disk_manager.WritePage(page.GetPageId(), page.GetData());

  quorumdb::Page read_page;
  disk_manager.ReadPage(page.GetPageId(), read_page.GetData());
  std::cout << read_page.GetData() << '\n';
  return 0;
}
