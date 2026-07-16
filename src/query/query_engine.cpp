#include "query/query_engine.h"

namespace quorumdb {

QueryEngine::QueryEngine(BPlusTree *index) : table_(index) {}

bool QueryEngine::Insert(std::int64_t id, const std::string &name, std::int64_t value) {
  return table_.Insert({id, name, value});
}

std::optional<Record> QueryEngine::SelectById(std::int64_t id) {
  Record record{};
  if (!table_.Select(id, &record)) {
    return std::nullopt;
  }
  return record;
}

std::vector<Record> QueryEngine::SelectWhere(
    const std::function<bool(const Record &)> &predicate) {
  return table_.SelectWhere(predicate);
}

bool QueryEngine::UpdateValue(std::int64_t id, std::int64_t new_value) {
  return table_.Update(id, [new_value](Record &record) { record.value = new_value; });
}

bool QueryEngine::DeleteById(std::int64_t id) { return table_.Delete(id); }

}  // namespace quorumdb
