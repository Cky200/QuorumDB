#pragma once

#include <functional>
#include <vector>

#include "query/record.h"
#include "storage/bplus_tree.h"
#include "storage/wal_manager.h"

namespace quorumdb {

class Table {
 public:
  explicit Table(BPlusTree *index, WALManager *wal_manager = nullptr);

  bool Insert(const Record &record);
  bool Select(std::int64_t id, Record *out);
  std::vector<Record> SelectWhere(const std::function<bool(const Record &)> &predicate);
  bool Update(std::int64_t id, const std::function<void(Record &)> &mutator);
  bool Delete(std::int64_t id);
  void Recover();

 private:
  bool ReadRecord(value_t record_page_id, Record *out);
  bool InsertWithoutWAL(const Record &record);
  bool WriteRecord(value_t record_page_id, const Record &record);
  bool UpdateWithoutWAL(std::int64_t id, const Record &record);
  bool DeleteWithoutWAL(std::int64_t id);
  static std::string RecordImage(const Record &record);
  static Record RecordFromImage(const std::string &image);

  BPlusTree *index_;
  BufferPoolManager *buffer_pool_manager_;
  WALManager *wal_manager_;
};

}  // namespace quorumdb
