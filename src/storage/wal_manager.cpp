#include "storage/wal_manager.h"

#include <stdexcept>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace quorumdb {

WALManager::WALManager(const std::string &log_file_path) : log_file_path_(log_file_path) {
  file_.open(log_file_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
  if (!file_.is_open()) {
    std::ofstream create(log_file_path_, std::ios::binary);
    if (!create) {
      throw std::runtime_error("failed to create WAL file");
    }
    create.close();
    file_.open(log_file_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
  }
  if (!file_.is_open()) {
    throw std::runtime_error("failed to open WAL file");
  }
  const auto records = ReadAllRecordsUnlocked();
  if (!records.empty()) {
    next_lsn_ = records.back().lsn + 1;
  }
}

WALManager::~WALManager() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_.is_open()) {
    file_.flush();
    file_.close();
  }
}

std::int64_t WALManager::AppendInsert(std::int64_t record_id, const std::string &after_image) {
  std::lock_guard<std::mutex> lock(mutex_);
  return Append(LogRecordType::INSERT, record_id, "", after_image);
}

std::int64_t WALManager::AppendUpdate(std::int64_t record_id, const std::string &before_image,
                                      const std::string &after_image) {
  std::lock_guard<std::mutex> lock(mutex_);
  return Append(LogRecordType::UPDATE, record_id, before_image, after_image);
}

std::int64_t WALManager::AppendDelete(std::int64_t record_id, const std::string &before_image) {
  std::lock_guard<std::mutex> lock(mutex_);
  return Append(LogRecordType::DELETE, record_id, before_image, "");
}

void WALManager::AppendCommit() {
  std::lock_guard<std::mutex> lock(mutex_);
  Append(LogRecordType::COMMIT, -1, "", "");
}

void WALManager::Flush() {
  std::lock_guard<std::mutex> lock(mutex_);
  file_.flush();
  if (!file_) {
    throw std::runtime_error("failed to flush WAL file");
  }
#if defined(__unix__) || defined(__APPLE__)
  const int descriptor = open(log_file_path_.c_str(), O_RDONLY);
  if (descriptor < 0 || fsync(descriptor) != 0) {
    if (descriptor >= 0) {
      close(descriptor);
    }
    throw std::runtime_error("failed to fsync WAL file");
  }
  close(descriptor);
#endif
}

std::vector<LogRecord> WALManager::ReadAllRecords() {
  std::lock_guard<std::mutex> lock(mutex_);
  file_.flush();
  return ReadAllRecordsUnlocked();
}

std::int64_t WALManager::Append(LogRecordType type, std::int64_t record_id,
                                const std::string &before_image, const std::string &after_image) {
  LogRecord record{next_lsn_++, type, record_id, before_image, after_image};
  const std::string bytes = SerializeLogRecord(record);
  const auto size = static_cast<std::uint32_t>(bytes.size());
  file_.write(reinterpret_cast<const char *>(&size), sizeof(size));
  file_.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  if (!file_) {
    throw std::runtime_error("failed to append WAL record");
  }
  return record.lsn;
}

std::vector<LogRecord> WALManager::ReadAllRecordsUnlocked() {
  std::vector<LogRecord> records;
  std::ifstream input(log_file_path_, std::ios::binary);
  if (!input) {
    throw std::runtime_error("failed to read WAL file");
  }
  while (true) {
    std::uint32_t size = 0;
    input.read(reinterpret_cast<char *>(&size), sizeof(size));
    if (input.eof()) {
      break;
    }
    if (!input || size == 0) {
      break;
    }
    std::string bytes(size, '\0');
    input.read(bytes.data(), static_cast<std::streamsize>(size));
    if (!input) {
      break;
    }
    LogRecord record{};
    if (!DeserializeLogRecord(bytes.data(), bytes.size(), &record)) {
      break;
    }
    records.push_back(std::move(record));
  }
  return records;
}

}  // namespace quorumdb
