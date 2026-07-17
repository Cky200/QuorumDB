#include "raft/raft_node.h"

#include <cstdlib>
#include <stdexcept>

#include "rpc/rpc_client.h"

namespace quorumdb {
namespace {

constexpr auto kHeartbeatInterval = std::chrono::milliseconds(75);
constexpr int kRpcTimeoutMs = 100;

}  // namespace

RaftNode::RaftNode(std::int32_t node_id,
                   std::vector<std::pair<std::int32_t, std::string>> peers, int listen_port)
    : node_id_(node_id),
      peers_(std::move(peers)),
      listen_port_(listen_port),
      cluster_size_(peers_.size() + 1),
      random_engine_(static_cast<std::mt19937::result_type>(
          std::chrono::steady_clock::now().time_since_epoch().count() ^ node_id_)) {}

RaftNode::~RaftNode() { Stop(); }

void RaftNode::Start() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) {
      return;
    }
    server_ = std::make_unique<RpcServer>(listen_port_,
                                           [this](const RpcMessage &message) {
                                             return HandleMessage(message);
                                           });
    server_->Start();
    running_ = true;
    state_ = RaftState::FOLLOWER;
    leader_id_ = -1;
    ResetElectionDeadlineLocked();
  }
  election_thread_ = std::thread(&RaftNode::ElectionLoop, this);
}

void RaftNode::Stop() {
  std::unique_ptr<RpcServer> server;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_ && !election_thread_.joinable()) {
      return;
    }
    running_ = false;
    condition_.notify_all();
    server = std::move(server_);
  }
  if (server != nullptr) {
    server->Stop();
  }
  if (election_thread_.joinable()) {
    election_thread_.join();
  }
  JoinRpcThreads();
}

RaftState RaftNode::GetState() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return state_;
}

std::int32_t RaftNode::GetCurrentTerm() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_term_;
}

RpcMessage RaftNode::HandleMessage(const RpcMessage &message) {
  if (message.type == RpcMessageType::REQUEST_VOTE) {
    return HandleRequestVote(message.term, message.sender_id);
  }
  if (message.type == RpcMessageType::PING) {
    // Placeholder heartbeat until AppendEntries is added in the next Raft stage.
    return HandleHeartbeat(message.term, message.sender_id);
  }
  return {RpcMessageType::PONG, GetCurrentTerm(), node_id_, ""};
}

RpcMessage RaftNode::HandleRequestVote(std::int32_t candidate_term, std::int32_t candidate_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  bool vote_granted = false;
  if (candidate_term >= current_term_) {
    if (candidate_term > current_term_) {
      current_term_ = candidate_term;
      voted_for_ = -1;
      state_ = RaftState::FOLLOWER;
      leader_id_ = -1;
    }
    ResetElectionDeadlineLocked();
    if (voted_for_ == -1 || voted_for_ == candidate_id) {
      voted_for_ = candidate_id;
      state_ = RaftState::FOLLOWER;
      leader_id_ = -1;
      vote_granted = true;
    }
  }
  return {RpcMessageType::REQUEST_VOTE_REPLY, current_term_, node_id_, vote_granted ? "1" : "0"};
}

RpcMessage RaftNode::HandleHeartbeat(std::int32_t leader_term, std::int32_t leader_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (leader_term >= current_term_) {
    if (leader_term > current_term_) {
      current_term_ = leader_term;
      voted_for_ = -1;
    }
    state_ = RaftState::FOLLOWER;
    leader_id_ = leader_id;
    ResetElectionDeadlineLocked();
  }
  return {RpcMessageType::PONG, current_term_, node_id_, ""};
}

void RaftNode::ElectionLoop() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (running_) {
    const auto wake_time = state_ == RaftState::LEADER ? next_heartbeat_ : election_deadline_;
    condition_.wait_until(lock, wake_time);
    if (!running_) {
      break;
    }
    const auto now = std::chrono::steady_clock::now();
    if (state_ == RaftState::LEADER && now >= next_heartbeat_) {
      next_heartbeat_ = now + kHeartbeatInterval;
      lock.unlock();
      SendHeartbeats();
      lock.lock();
    } else if (state_ != RaftState::LEADER && now >= election_deadline_) {
      lock.unlock();
      StartElection();
      lock.lock();
    }
  }
}

void RaftNode::StartElection() {
  std::int32_t election_term = 0;
  bool became_leader = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_ || state_ == RaftState::LEADER) {
      return;
    }
    state_ = RaftState::CANDIDATE;
    ++current_term_;
    voted_for_ = node_id_;
    leader_id_ = -1;
    votes_received_.clear();
    votes_received_.insert(node_id_);
    election_term = current_term_;
    ResetElectionDeadlineLocked();
    became_leader = votes_received_.size() > cluster_size_ / 2;
    if (became_leader) {
      state_ = RaftState::LEADER;
      leader_id_ = node_id_;
      next_heartbeat_ = std::chrono::steady_clock::now();
      condition_.notify_all();
    }
  }
  if (became_leader) {
    SendHeartbeats();
    return;
  }

  std::lock_guard<std::mutex> lock(rpc_threads_mutex_);
  {
    std::lock_guard<std::mutex> state_lock(mutex_);
    if (!running_) {
      return;
    }
  }
  ReapRpcThreadsLocked();

  for (const auto &peer : peers_) {
    auto finished = std::make_shared<std::atomic<bool>>(false);
    rpc_threads_.push_back({
        std::thread([this, election_term, peer, finished]() {
          std::string host;
          int port = 0;
          if (ParsePeerAddress(peer.second, &host, &port)) {
            RpcClient client;
            RpcMessage reply{};
            const RpcMessage request{RpcMessageType::REQUEST_VOTE, election_term, node_id_, ""};
            if (client.SendMessage(host, port, request, &reply, kRpcTimeoutMs)) {
              HandleVoteReply(election_term, peer.first, reply);
            }
          }
          finished->store(true);
        }),
        finished
    });
  }
}

void RaftNode::HandleVoteReply(std::int32_t election_term, std::int32_t peer_id,
                               const RpcMessage &reply) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!running_) {
    return;
  }
  if (reply.term > current_term_) {
    current_term_ = reply.term;
    voted_for_ = -1;
    state_ = RaftState::FOLLOWER;
    leader_id_ = -1;
    ResetElectionDeadlineLocked();
    return;
  }
  if (state_ != RaftState::CANDIDATE || current_term_ != election_term ||
      reply.type != RpcMessageType::REQUEST_VOTE_REPLY || reply.term != election_term ||
      reply.payload != "1") {
    return;
  }
  votes_received_.insert(peer_id);
  if (votes_received_.size() > cluster_size_ / 2) {
    state_ = RaftState::LEADER;
    leader_id_ = node_id_;
    next_heartbeat_ = std::chrono::steady_clock::now();
    condition_.notify_all();
  }
}

void RaftNode::SendHeartbeats() {
  std::int32_t term = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_ || state_ != RaftState::LEADER) {
      return;
    }
    term = current_term_;
  }

  std::lock_guard<std::mutex> lock(rpc_threads_mutex_);
  {
    std::lock_guard<std::mutex> state_lock(mutex_);
    if (!running_) {
      return;
    }
  }
  ReapRpcThreadsLocked();

  for (const auto &peer : peers_) {
    auto finished = std::make_shared<std::atomic<bool>>(false);
    rpc_threads_.push_back({
        std::thread([this, term, peer, finished]() {
          std::string host;
          int port = 0;
          if (ParsePeerAddress(peer.second, &host, &port)) {
            RpcClient client;
            RpcMessage ignored{};
            // PING is deliberately a temporary heartbeat stand-in for AppendEntries.
            client.SendMessage(host, port, {RpcMessageType::PING, term, node_id_, ""}, &ignored,
                               kRpcTimeoutMs);
          }
          finished->store(true);
        }),
        finished
    });
  }
}

void RaftNode::ResetElectionDeadlineLocked() {
  election_deadline_ = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(election_timeout_ms_(random_engine_));
}

void RaftNode::JoinRpcThreads() {
  std::vector<ActiveThread> threads;
  {
    std::lock_guard<std::mutex> lock(rpc_threads_mutex_);
    threads.swap(rpc_threads_);
  }
  for (auto &t : threads) {
    if (t.thread.joinable()) {
      t.thread.join();
    }
  }
}

void RaftNode::ReapRpcThreadsLocked() {
  for (auto it = rpc_threads_.begin(); it != rpc_threads_.end(); ) {
    if (it->finished->load()) {
      if (it->thread.joinable()) {
        it->thread.join();
      }
      it = rpc_threads_.erase(it);
    } else {
      ++it;
    }
  }
}

bool RaftNode::ParsePeerAddress(const std::string &address, std::string *host, int *port) {
  const auto separator = address.rfind(':');
  if (host == nullptr || port == nullptr || separator == std::string::npos || separator == 0 ||
      separator + 1 == address.size()) {
    return false;
  }
  try {
    const int parsed_port = std::stoi(address.substr(separator + 1));
    if (parsed_port <= 0 || parsed_port > 65535) {
      return false;
    }
    *host = address.substr(0, separator);
    *port = parsed_port;
    return true;
  } catch (const std::exception &) {
    return false;
  }
}

}  // namespace quorumdb
