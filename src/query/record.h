#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace quorumdb {

struct Record {
  std::int64_t id;
  std::string name;
  std::int64_t value;
};

constexpr std::size_t RECORD_NAME_SIZE = 64;
constexpr std::size_t RECORD_SIZE = sizeof(std::int64_t) + RECORD_NAME_SIZE + sizeof(std::int64_t);

inline void SerializeRecord(const Record &record, char *buffer) {
  std::memset(buffer, 0, RECORD_SIZE);
  std::memcpy(buffer, &record.id, sizeof(record.id));
  const std::size_t name_size = std::min(record.name.size(), RECORD_NAME_SIZE);
  std::memcpy(buffer + sizeof(record.id), record.name.data(), name_size);
  std::memcpy(buffer + sizeof(record.id) + RECORD_NAME_SIZE, &record.value, sizeof(record.value));
}

inline Record DeserializeRecord(const char *buffer) {
  Record record{};
  std::memcpy(&record.id, buffer, sizeof(record.id));
  const char *name_begin = buffer + sizeof(record.id);
  const auto name_end = std::find(name_begin, name_begin + RECORD_NAME_SIZE, '\0');
  record.name.assign(name_begin, name_end);
  std::memcpy(&record.value, buffer + sizeof(record.id) + RECORD_NAME_SIZE, sizeof(record.value));
  return record;
}

}  // namespace quorumdb
