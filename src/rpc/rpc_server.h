#pragma once

#include <functional>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "rpc/rpc_message.h"

namespace quorumdb {

class RpcServer {
 public:
  using MessageHandler = std::function<RpcMessage(const RpcMessage &)>;

  RpcServer(int port, MessageHandler handler);
  ~RpcServer();

  RpcServer(const RpcServer &) = delete;
  RpcServer &operator=(const RpcServer &) = delete;

  void Start();
  void Stop();
  int GetPort() const;

 private:
  void AcceptLoop();
  void HandleConnection(int connection_fd);

  int port_;
  MessageHandler handler_;
  int listen_fd_{-1};
  bool running_{false};
  std::thread accept_thread_;
  std::vector<std::thread> client_threads_;
  std::unordered_set<int> active_connections_;
  mutable std::mutex mutex_;
  std::mutex client_threads_mutex_;
};

}  // namespace quorumdb
