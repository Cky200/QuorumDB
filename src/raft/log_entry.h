#pragma once

#include <arpa/inet.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace quorumdb {

struct LogEntry {
  std::int32_t term;
  std::int64_t index;
  std::string command;
};

struct AppendEntriesArgs {
  std::int64_t prev_log_index;
  std::int32_t prev_log_term;
  std::int64_t leader_commit;
  std::vector<LogEntry> entries;
};

inline std::string SerializeAppendEntriesPayload(const AppendEntriesArgs& args) {
  std::size_t size = sizeof(std::uint32_t) * 4 + sizeof(std::uint32_t); 
  // prev_log_index (2 * u32), prev_log_term (u32), leader_commit (2 * u32), entries_count (u32)
  size += sizeof(std::uint32_t) * 2; // Extra to account for 64 bit fields

  for (const auto& entry : args.entries) {
    size += sizeof(std::uint32_t) * 4 + entry.command.size(); // term(u32), index(2*u32), cmd_len(u32)
  }

  std::string bytes(size, '\0');
  std::size_t offset = 0;

  auto write_u32 = [&bytes, &offset](std::uint32_t val) {
    std::uint32_t net_val = htonl(val);
    std::memcpy(bytes.data() + offset, &net_val, sizeof(net_val));
    offset += sizeof(net_val);
  };

  auto write_u64 = [&write_u32](std::uint64_t val) {
    write_u32(static_cast<std::uint32_t>(val >> 32));
    write_u32(static_cast<std::uint32_t>(val & 0xFFFFFFFFULL));
  };

  write_u64(static_cast<std::uint64_t>(args.prev_log_index));
  write_u32(static_cast<std::uint32_t>(args.prev_log_term));
  write_u64(static_cast<std::uint64_t>(args.leader_commit));
  write_u32(static_cast<std::uint32_t>(args.entries.size()));

  for (const auto& entry : args.entries) {
    write_u32(static_cast<std::uint32_t>(entry.term));
    write_u64(static_cast<std::uint64_t>(entry.index));
    write_u32(static_cast<std::uint32_t>(entry.command.size()));
    std::memcpy(bytes.data() + offset, entry.command.data(), entry.command.size());
    offset += entry.command.size();
  }

  return bytes;
}

inline bool DeserializeAppendEntriesPayload(const std::string& bytes, AppendEntriesArgs* args) {
  if (args == nullptr || bytes.size() < sizeof(std::uint32_t) * 6) {
    return false;
  }

  std::size_t offset = 0;

  auto read_u32 = [&bytes, &offset]() -> std::uint32_t {
    std::uint32_t net_val;
    std::memcpy(&net_val, bytes.data() + offset, sizeof(net_val));
    offset += sizeof(net_val);
    return ntohl(net_val);
  };

  auto read_u64 = [&read_u32]() -> std::uint64_t {
    std::uint64_t high = read_u32();
    std::uint64_t low = read_u32();
    return (high << 32) | low;
  };

  args->prev_log_index = static_cast<std::int64_t>(read_u64());
  args->prev_log_term = static_cast<std::int32_t>(read_u32());
  args->leader_commit = static_cast<std::int64_t>(read_u64());
  
  std::uint32_t entries_count = read_u32();
  args->entries.clear();
  args->entries.reserve(entries_count);

  for (std::uint32_t i = 0; i < entries_count; ++i) {
    if (offset + sizeof(std::uint32_t) * 4 > bytes.size()) {
      return false;
    }
    LogEntry entry;
    entry.term = static_cast<std::int32_t>(read_u32());
    entry.index = static_cast<std::int64_t>(read_u64());
    std::uint32_t cmd_len = read_u32();

    if (offset + cmd_len > bytes.size()) {
      return false;
    }
    entry.command.assign(bytes.data() + offset, cmd_len);
    offset += cmd_len;
    args->entries.push_back(std::move(entry));
  }

  return true;
}

}  // namespace quorumdb
