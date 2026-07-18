#pragma once

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <string>

namespace quorumdb {

enum class CommandType : std::uint8_t { INSERT, UPDATE_VALUE, DELETE };

struct RaftCommand {
  CommandType type;
  std::int64_t id;
  std::string name;       // INSERT only
  std::int64_t value;     // INSERT / UPDATE_VALUE only
};

inline std::string Serialize(const RaftCommand& cmd) {
  std::size_t size = sizeof(std::uint8_t) + sizeof(std::uint32_t) * 5 + cmd.name.size();
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

  bytes[offset++] = static_cast<char>(cmd.type);
  write_u64(static_cast<std::uint64_t>(cmd.id));
  write_u64(static_cast<std::uint64_t>(cmd.value));
  write_u32(static_cast<std::uint32_t>(cmd.name.size()));
  std::memcpy(bytes.data() + offset, cmd.name.data(), cmd.name.size());

  return bytes;
}

inline RaftCommand Deserialize(const std::string& bytes) {
  RaftCommand cmd{};
  if (bytes.size() < sizeof(std::uint8_t) + sizeof(std::uint32_t) * 5) {
    return cmd;
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

  cmd.type = static_cast<CommandType>(bytes[offset++]);
  cmd.id = static_cast<std::int64_t>(read_u64());
  cmd.value = static_cast<std::int64_t>(read_u64());
  std::uint32_t name_size = read_u32();
  
  if (offset + name_size <= bytes.size()) {
    cmd.name.assign(bytes.data() + offset, name_size);
  }
  
  return cmd;
}

}  // namespace quorumdb
