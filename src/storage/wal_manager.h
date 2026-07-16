#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

#include "storage/log_record.h"

namespace quorumdb {

class WALManager {
 public:
  explicit WALManager(const std::string &log_file_path);
  ~WALManager();

  WALManager(const WALManager &) = delete;
  WALManager &operator=(const WALManager &) = delete;

  std::int64_t AppendInsert(std::int64_t record_id, const std::string &after_image);
  std::int64_t AppendUpdate(std::int64_t record_id, const std::string &before_image,
                            const std::string &after_image);
  std::int64_t AppendDelete(std::int64_t record_id, const std::string &before_image);
  void AppendCommit();
  void Flush();
  std::vector<LogRecord> ReadAllRecords();

 private:
  std::int64_t Append(LogRecordType type, std::int64_t record_id, const std::string &before_image,
                      const std::string &after_image);
  std::vector<LogRecord> ReadAllRecordsUnlocked();

  std::string log_file_path_;
  std::fstream file_;
  std::int64_t next_lsn_{0};
  mutable std::mutex mutex_;
};

}  // namespace quorumdb
