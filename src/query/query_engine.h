#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "query/table.h"

namespace quorumdb {

class QueryEngine {
 public:
  explicit QueryEngine(BPlusTree *index);

  bool Insert(std::int64_t id, const std::string &name, std::int64_t value);
  std::optional<Record> SelectById(std::int64_t id);
  std::vector<Record> SelectWhere(const std::function<bool(const Record &)> &predicate);
  bool UpdateValue(std::int64_t id, std::int64_t new_value);
  bool DeleteById(std::int64_t id);

 private:
  Table table_;
};

}  // namespace quorumdb
