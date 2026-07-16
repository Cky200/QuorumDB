#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace quorumdb {

enum class LogRecordType : std::uint8_t { INSERT, UPDATE, DELETE, COMMIT };

struct LogRecord {
  std::int64_t lsn;
  LogRecordType type;
  std::int64_t record_id;
  std::string before_image;
  std::string after_image;
};

inline std::string SerializeLogRecord(const LogRecord &record) {
  const auto before_size = static_cast<std::uint32_t>(record.before_image.size());
  const auto after_size = static_cast<std::uint32_t>(record.after_image.size());
  std::string bytes(sizeof(record.lsn) + sizeof(std::uint8_t) + sizeof(record.record_id) +
                        sizeof(before_size) + sizeof(after_size) + before_size + after_size,
                    '\0');
  std::size_t offset = 0;
  const auto write = [&bytes, &offset](const auto &value) {
    std::memcpy(bytes.data() + offset, &value, sizeof(value));
    offset += sizeof(value);
  };
  const auto type = static_cast<std::uint8_t>(record.type);
  write(record.lsn);
  write(type);
  write(record.record_id);
  write(before_size);
  write(after_size);
  std::memcpy(bytes.data() + offset, record.before_image.data(), before_size);
  offset += before_size;
  std::memcpy(bytes.data() + offset, record.after_image.data(), after_size);
  return bytes;
}

inline bool DeserializeLogRecord(const char *bytes, std::size_t size, LogRecord *record) {
  constexpr std::size_t kHeaderSize = sizeof(std::int64_t) + sizeof(std::uint8_t) +
                                      sizeof(std::int64_t) + sizeof(std::uint32_t) * 2;
  if (record == nullptr || size < kHeaderSize) {
    return false;
  }
  std::size_t offset = 0;
  const auto read = [bytes, &offset](auto *value) {
    std::memcpy(value, bytes + offset, sizeof(*value));
    offset += sizeof(*value);
  };
  std::uint8_t type = 0;
  std::uint32_t before_size = 0;
  std::uint32_t after_size = 0;
  read(&record->lsn);
  read(&type);
  read(&record->record_id);
  read(&before_size);
  read(&after_size);
  if (type > static_cast<std::uint8_t>(LogRecordType::COMMIT) ||
      before_size > size - offset || after_size > size - offset - before_size) {
    return false;
  }
  record->type = static_cast<LogRecordType>(type);
  record->before_image.assign(bytes + offset, before_size);
  offset += before_size;
  record->after_image.assign(bytes + offset, after_size);
  return offset + after_size == size;
}

}  // namespace quorumdb
