#pragma once

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace quorumdb {

enum class RpcMessageType : std::uint8_t {
  REQUEST_VOTE,
  REQUEST_VOTE_REPLY,
  APPEND_ENTRIES,
  APPEND_ENTRIES_REPLY,
  PING,
  PONG,
};

struct RpcMessage {
  RpcMessageType type;
  std::int32_t term;
  std::int32_t sender_id;
  std::string payload;
};

inline std::string SerializeRpcMessage(const RpcMessage &message) {
  const auto payload_size = static_cast<std::uint32_t>(message.payload.size());
  const std::uint32_t body_size = static_cast<std::uint32_t>(sizeof(std::uint8_t) +
                                                              sizeof(std::uint32_t) * 3 + payload_size);
  std::string bytes(sizeof(std::uint32_t) + body_size, '\0');
  std::size_t offset = 0;
  const auto write_u32 = [&bytes, &offset](std::uint32_t value) {
    const std::uint32_t network_value = htonl(value);
    std::memcpy(bytes.data() + offset, &network_value, sizeof(network_value));
    offset += sizeof(network_value);
  };
  write_u32(body_size);
  bytes[offset++] = static_cast<char>(message.type);
  write_u32(static_cast<std::uint32_t>(message.term));
  write_u32(static_cast<std::uint32_t>(message.sender_id));
  write_u32(payload_size);
  std::memcpy(bytes.data() + offset, message.payload.data(), payload_size);
  return bytes;
}

inline bool DeserializeRpcMessage(const char *bytes, std::size_t size, RpcMessage *message) {
  constexpr std::size_t kHeaderSize = sizeof(std::uint32_t) + sizeof(std::uint8_t) +
                                      sizeof(std::uint32_t) * 3;
  if (message == nullptr || size < kHeaderSize) {
    return false;
  }
  std::size_t offset = 0;
  const auto read_u32 = [bytes, &offset]() {
    std::uint32_t network_value = 0;
    std::memcpy(&network_value, bytes + offset, sizeof(network_value));
    offset += sizeof(network_value);
    return ntohl(network_value);
  };
  const std::uint32_t body_size = read_u32();
  if (body_size != size - sizeof(std::uint32_t)) {
    return false;
  }
  const auto type = static_cast<std::uint8_t>(bytes[offset++]);
  if (type > static_cast<std::uint8_t>(RpcMessageType::PONG)) {
    return false;
  }
  message->type = static_cast<RpcMessageType>(type);
  message->term = static_cast<std::int32_t>(read_u32());
  message->sender_id = static_cast<std::int32_t>(read_u32());
  const std::uint32_t payload_size = read_u32();
  if (payload_size != size - offset) {
    return false;
  }
  message->payload.assign(bytes + offset, payload_size);
  return true;
}

inline bool DeserializeRpcMessage(const std::string &bytes, RpcMessage *message) {
  return DeserializeRpcMessage(bytes.data(), bytes.size(), message);
}

}  // namespace quorumdb
