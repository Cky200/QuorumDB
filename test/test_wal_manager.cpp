#include <cassert>
#include <cstdio>
#include <string>
#include <vector>

#include "query/table.h"
#include "storage/wal_manager.h"

namespace {

std::string TestFilename(const char *name, const char *extension) {
  return std::string("test_wal_") + name + extension;
}

void RemoveFile(const std::string &filename) { std::remove(filename.c_str()); }

void TestAppendOrderAndContents() {
  const auto filename = TestFilename("records", ".log");
  RemoveFile(filename);
  {
    quorumdb::WALManager wal(filename);
    const auto insert_lsn = wal.AppendInsert(1, "after-insert");
    const auto update_lsn = wal.AppendUpdate(1, "before-update", "after-update");
    const auto delete_lsn = wal.AppendDelete(1, "before-delete");
    assert(insert_lsn < update_lsn && update_lsn < delete_lsn);
    wal.Flush();

    const auto records = wal.ReadAllRecords();
    assert(records.size() == 3);
    assert(records[0].lsn == insert_lsn);
    assert(records[0].type == quorumdb::LogRecordType::INSERT);
    assert(records[0].record_id == 1);
    assert(records[0].before_image.empty());
    assert(records[0].after_image == "after-insert");
    assert(records[1].lsn == update_lsn);
    assert(records[1].type == quorumdb::LogRecordType::UPDATE);
    assert(records[1].before_image == "before-update");
    assert(records[1].after_image == "after-update");
    assert(records[2].lsn == delete_lsn);
    assert(records[2].type == quorumdb::LogRecordType::DELETE);
    assert(records[2].before_image == "before-delete");
    assert(records[2].after_image.empty());
  }
  RemoveFile(filename);
}

void TestRecoveryAndIdempotency() {
  const auto database_filename = TestFilename("recovery", ".db");
  const auto log_filename = TestFilename("recovery", ".log");
  RemoveFile(database_filename);
  RemoveFile(log_filename);
  {
    quorumdb::DiskManager disk_manager(database_filename);
    quorumdb::BufferPoolManager buffer_pool(32, &disk_manager);
    quorumdb::BPlusTree tree(&buffer_pool);
    quorumdb::WALManager wal(log_filename);
    quorumdb::Table table(&tree, &wal);
    assert(table.Insert({1, "Ada", 100}));
    assert(table.Insert({2, "Grace", 200}));
    assert(table.Insert({3, "Linus", 300}));
  }

  {
    quorumdb::DiskManager disk_manager(database_filename);
    quorumdb::BufferPoolManager buffer_pool(32, &disk_manager);
    quorumdb::BPlusTree tree(&buffer_pool);
    quorumdb::WALManager wal(log_filename);
    quorumdb::Table table(&tree, &wal);
    table.Recover();
    for (const auto &expected : std::vector<quorumdb::Record>{{1, "Ada", 100}, {2, "Grace", 200},
                                                               {3, "Linus", 300}}) {
      quorumdb::Record actual{};
      assert(table.Select(expected.id, &actual));
      assert(actual.id == expected.id);
      assert(actual.name == expected.name);
      assert(actual.value == expected.value);
    }

    table.Recover();
    const auto all_records = table.SelectWhere([](const quorumdb::Record &) { return true; });
    assert(all_records.size() == 3);
    assert(all_records[0].id == 1 && all_records[0].value == 100);
    assert(all_records[1].id == 2 && all_records[1].value == 200);
    assert(all_records[2].id == 3 && all_records[2].value == 300);
  }
  RemoveFile(database_filename);
  RemoveFile(log_filename);
}

}  // namespace

int main() {
  TestAppendOrderAndContents();
  TestRecoveryAndIdempotency();
  return 0;
}
