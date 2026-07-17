#include "rpc/rpc_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
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

}  // namespace

RpcServer::RpcServer(int port, MessageHandler handler) : port_(port), handler_(std::move(handler)) {
  if (port_ < 0 || port_ > 65535 || !handler_) {
    throw std::invalid_argument("invalid RPC server configuration");
  }
}

RpcServer::~RpcServer() { Stop(); }

void RpcServer::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (running_) {
    return;
  }
  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    throw std::runtime_error("failed to create RPC socket");
  }
  int reuse_address = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse_address, sizeof(reuse_address));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(static_cast<std::uint16_t>(port_));
  if (bind(listen_fd_, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) != 0 ||
      listen(listen_fd_, SOMAXCONN) != 0) {
    close(listen_fd_);
    listen_fd_ = -1;
    throw std::runtime_error("failed to bind or listen on RPC socket");
  }
  sockaddr_in bound_address{};
  socklen_t bound_address_size = sizeof(bound_address);
  if (getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&bound_address), &bound_address_size) != 0) {
    close(listen_fd_);
    listen_fd_ = -1;
    throw std::runtime_error("failed to determine RPC server port");
  }
  port_ = ntohs(bound_address.sin_port);
  running_ = true;
  accept_thread_ = std::thread(&RpcServer::AcceptLoop, this);
}

int RpcServer::GetPort() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return port_;
}

void RpcServer::Stop() {
  int listener = -1;
  std::vector<int> connections;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_ && !accept_thread_.joinable()) {
      return;
    }
    running_ = false;
    listener = listen_fd_;
    listen_fd_ = -1;
    connections.assign(active_connections_.begin(), active_connections_.end());
  }
  if (listener >= 0) {
    shutdown(listener, SHUT_RDWR);
    close(listener);
  }
  for (int connection : connections) {
    shutdown(connection, SHUT_RDWR);
  }
  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }
  std::vector<std::thread> client_threads;
  {
    std::lock_guard<std::mutex> lock(client_threads_mutex_);
    client_threads.swap(client_threads_);
  }
  for (auto &thread : client_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

void RpcServer::AcceptLoop() {
  while (true) {
    int listener = -1;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!running_) {
        return;
      }
      listener = listen_fd_;
    }
    fd_set read_set;
    FD_ZERO(&read_set);
    FD_SET(listener, &read_set);
    timeval timeout{0, 100000};
    const int ready = select(listener + 1, &read_set, nullptr, nullptr, &timeout);
    if (ready == 0) {
      continue;
    }
    if (ready < 0) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!running_) {
        return;
      }
      continue;
    }
    const int connection_fd = accept(listener, nullptr, nullptr);
    if (connection_fd < 0) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!running_) {
        return;
      }
      if (errno == EINTR) {
        continue;
      }
      continue;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!running_) {
        close(connection_fd);
        return;
      }
      active_connections_.insert(connection_fd);
    }
    std::lock_guard<std::mutex> lock(client_threads_mutex_);
    client_threads_.emplace_back(&RpcServer::HandleConnection, this, connection_fd);
  }
}

void RpcServer::HandleConnection(int connection_fd) {
  RpcMessage request{};
  if (ReceiveMessage(connection_fd, &request)) {
    const RpcMessage reply = handler_(request);
    const std::string bytes = SerializeRpcMessage(reply);
    SendAll(connection_fd, bytes.data(), bytes.size());
  }
  shutdown(connection_fd, SHUT_RDWR);
  close(connection_fd);
  std::lock_guard<std::mutex> lock(mutex_);
  active_connections_.erase(connection_fd);
}

}  // namespace quorumdb
