#include "rpc/rpc_client.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>

namespace quorumdb {
namespace {

constexpr std::uint32_t kMaxMessageSize = 16U * 1024U * 1024U;

bool SendAll(int fd, const char *data, std::size_t size) {
  std::size_t sent = 0;
  while (sent < size) {
    const auto result = send(fd, data + sent, size - sent, 0);
    if (result <= 0) {
      return false;
    }
    sent += static_cast<std::size_t>(result);
  }
  return true;
}

bool ReceiveAll(int fd, char *data, std::size_t size) {
  std::size_t received = 0;
  while (received < size) {
    const auto result = recv(fd, data + received, size - received, 0);
    if (result <= 0) {
      return false;
    }
    received += static_cast<std::size_t>(result);
  }
  return true;
}

bool ReceiveMessage(int fd, RpcMessage *message) {
  std::uint32_t network_size = 0;
  if (!ReceiveAll(fd, reinterpret_cast<char *>(&network_size), sizeof(network_size))) {
    return false;
  }
  const std::uint32_t body_size = ntohl(network_size);
  if (body_size < 13 || body_size > kMaxMessageSize) {
    return false;
  }
  std::string bytes(sizeof(network_size) + body_size, '\0');
  std::memcpy(bytes.data(), &network_size, sizeof(network_size));
  if (!ReceiveAll(fd, bytes.data() + sizeof(network_size), body_size)) {
    return false;
  }
  return DeserializeRpcMessage(bytes, message);
}

bool WaitForWritable(int fd, int timeout_ms) {
  fd_set write_set;
  FD_ZERO(&write_set);
  FD_SET(fd, &write_set);
  timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
  return select(fd + 1, nullptr, &write_set, nullptr, &timeout) > 0 && FD_ISSET(fd, &write_set);
}

}  // namespace

bool RpcClient::SendMessage(const std::string &host, int port, const RpcMessage &message,
                            RpcMessage *out_reply, int timeout_ms) const {
  if (out_reply == nullptr || port <= 0 || port > 65535 || timeout_ms < 0) {
    return false;
  }
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo *addresses = nullptr;
  if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &addresses) != 0) {
    return false;
  }

  int socket_fd = -1;
  for (addrinfo *address = addresses; address != nullptr; address = address->ai_next) {
    socket_fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (socket_fd < 0) {
      continue;
    }
    const int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags < 0 || fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      close(socket_fd);
      socket_fd = -1;
      continue;
    }
    const int result = connect(socket_fd, address->ai_addr, address->ai_addrlen);
    if (result != 0 && (errno != EINPROGRESS || !WaitForWritable(socket_fd, timeout_ms))) {
      close(socket_fd);
      socket_fd = -1;
      continue;
    }
    int connection_error = 0;
    socklen_t error_size = sizeof(connection_error);
    if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &connection_error, &error_size) != 0 ||
        connection_error != 0 || fcntl(socket_fd, F_SETFL, flags) < 0) {
      close(socket_fd);
      socket_fd = -1;
      continue;
    }
    break;
  }
  freeaddrinfo(addresses);
  if (socket_fd < 0) {
    return false;
  }

  timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
  setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  const std::string bytes = SerializeRpcMessage(message);
  const bool success = SendAll(socket_fd, bytes.data(), bytes.size()) && ReceiveMessage(socket_fd, out_reply);
  shutdown(socket_fd, SHUT_RDWR);
  close(socket_fd);
  return success;
}

}  // namespace quorumdb
