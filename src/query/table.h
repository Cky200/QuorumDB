#pragma once

#include <functional>
#include <vector>

#include "query/record.h"
#include "storage/bplus_tree.h"

namespace quorumdb {

class Table {
 public:
  explicit Table(BPlusTree *index);

  bool Insert(const Record &record);
  bool Select(std::int64_t id, Record *out);
  std::vector<Record> SelectWhere(const std::function<bool(const Record &)> &predicate);
  bool Update(std::int64_t id, const std::function<void(Record &)> &mutator);
  bool Delete(std::int64_t id);

 private:
  bool ReadRecord(value_t record_page_id, Record *out);

  BPlusTree *index_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace quorumdb
