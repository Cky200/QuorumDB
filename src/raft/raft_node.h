#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "raft/log_entry.h"
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
  std::vector<LogEntry> GetLog() const;
  std::int64_t GetCommitIndex() const;
  bool Propose(const std::string &command);
  void SetOnCommitCallback(std::function<void(const std::string &, std::int64_t)> callback);

 private:
  RpcMessage HandleMessage(const RpcMessage &message);
  RpcMessage HandleRequestVote(std::int32_t candidate_term, std::int32_t candidate_id);
  RpcMessage HandleAppendEntries(std::int32_t term, std::int32_t leader_id,
                                 std::int64_t prev_log_index, std::int32_t prev_log_term,
                                 std::int64_t leader_commit, const std::vector<LogEntry> &entries);
  void ElectionLoop();
  void StartElection();
  void HandleVoteReply(std::int32_t election_term, std::int32_t peer_id,
                       const RpcMessage &reply);
  void HandleAppendEntriesReply(std::int32_t peer_id, const RpcMessage &reply, std::int64_t sent_match_index);
  void SendHeartbeats();
  void ResetElectionDeadlineLocked();
  void ReapRpcThreadsLocked();
  void JoinRpcThreads();
  static bool ParsePeerAddress(const std::string &address, std::string *host, int *port);

  void AdvanceCommitIndexLocked();
  void ApplyCommittedEntriesLocked();

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

  std::vector<LogEntry> log_{{0, 0, ""}};
  std::int64_t commit_index_{0};
  std::int64_t last_applied_{0};
  std::unordered_map<std::int32_t, std::int64_t> next_index_;
  std::unordered_map<std::int32_t, std::int64_t> match_index_;
  std::function<void(const std::string &, std::int64_t)> on_commit_;

  std::unique_ptr<RpcServer> server_;
  std::thread election_thread_;
  std::vector<ActiveThread> rpc_threads_;
  bool running_{false};
  mutable std::mutex mutex_;
  std::mutex rpc_threads_mutex_;
  std::condition_variable condition_;
};

}  // namespace quorumdb
