#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "rpc/rpc_server.h"

namespace quorumdb {

enum class RaftState { FOLLOWER, CANDIDATE, LEADER };

class RaftNode {
 public:
  RaftNode(std::int32_t node_id, std::vector<std::pair<std::int32_t, std::string>> peers,
           int listen_port);
  ~RaftNode();

  RaftNode(const RaftNode &) = delete;
  RaftNode &operator=(const RaftNode &) = delete;

  void Start();
  void Stop();

  RaftState GetState() const;
  std::int32_t GetCurrentTerm() const;

 private:
  RpcMessage HandleMessage(const RpcMessage &message);
  RpcMessage HandleRequestVote(std::int32_t candidate_term, std::int32_t candidate_id);
  RpcMessage HandleHeartbeat(std::int32_t leader_term, std::int32_t leader_id);
  void ElectionLoop();
  void StartElection();
  void HandleVoteReply(std::int32_t election_term, std::int32_t peer_id,
                       const RpcMessage &reply);
  void SendHeartbeats();
  void ResetElectionDeadlineLocked();
  void ReapRpcThreadsLocked();
  void JoinRpcThreads();
  static bool ParsePeerAddress(const std::string &address, std::string *host, int *port);

  struct ActiveThread {
    std::thread thread;
    std::shared_ptr<std::atomic<bool>> finished;
  };

  const std::int32_t node_id_;
  const std::vector<std::pair<std::int32_t, std::string>> peers_;
  const int listen_port_;
  const std::size_t cluster_size_;

  std::int32_t current_term_{0};
  std::int32_t voted_for_{-1};
  RaftState state_{RaftState::FOLLOWER};
  std::int32_t leader_id_{-1};
  std::unordered_set<std::int32_t> votes_received_;
  std::chrono::steady_clock::time_point election_deadline_;
  std::chrono::steady_clock::time_point next_heartbeat_;
  std::mt19937 random_engine_;
  std::uniform_int_distribution<int> election_timeout_ms_{150, 300};

  std::unique_ptr<RpcServer> server_;
  std::thread election_thread_;
  std::vector<ActiveThread> rpc_threads_;
  bool running_{false};
  mutable std::mutex mutex_;
  std::mutex rpc_threads_mutex_;
  std::condition_variable condition_;
};

}  // namespace quorumdb
