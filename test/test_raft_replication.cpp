#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "raft/raft_node.h"

namespace {

int ReserveLoopbackPort() {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  assert(fd >= 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  assert(bind(fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) == 0);
  socklen_t address_size = sizeof(address);
  assert(getsockname(fd, reinterpret_cast<sockaddr *>(&address), &address_size) == 0);
  const int port = ntohs(address.sin_port);
  close(fd);
  return port;
}

std::vector<std::pair<std::int32_t, std::string>> PeersFor(
    std::int32_t node_id, const std::vector<int> &ports) {
  std::vector<std::pair<std::int32_t, std::string>> peers;
  for (std::size_t index = 0; index < ports.size(); ++index) {
    const auto peer_id = static_cast<std::int32_t>(index + 1);
    if (peer_id != node_id) {
      peers.emplace_back(peer_id, "127.0.0.1:" + std::to_string(ports[index]));
    }
  }
  return peers;
}

int WaitForSingleLeader(const std::vector<quorumdb::RaftNode *> &nodes) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (std::chrono::steady_clock::now() < deadline) {
    int leaders = 0;
    int leader_idx = -1;
    for (std::size_t index = 0; index < nodes.size(); ++index) {
      if (nodes[index]->GetState() == quorumdb::RaftState::LEADER) {
        ++leaders;
        leader_idx = static_cast<int>(index);
      }
    }
    if (leaders == 1) {
      return leader_idx;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return -1;
}

void WaitForCommitIndex(const std::vector<quorumdb::RaftNode *> &nodes, std::int64_t expected_commit_index) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (std::chrono::steady_clock::now() < deadline) {
    bool all_match = true;
    for (const auto *node : nodes) {
      if (node->GetCommitIndex() < expected_commit_index) {
        all_match = false;
        break;
      }
    }
    if (all_match) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
}

void TestLogReplicationAndCommit() {
  const std::vector<int> ports{ReserveLoopbackPort(), ReserveLoopbackPort(), ReserveLoopbackPort()};
  quorumdb::RaftNode first(1, PeersFor(1, ports), ports[0]);
  quorumdb::RaftNode second(2, PeersFor(2, ports), ports[1]);
  quorumdb::RaftNode third(3, PeersFor(3, ports), ports[2]);
  std::vector<quorumdb::RaftNode *> nodes{&first, &second, &third};

  std::vector<std::string> first_commits;
  first.SetOnCommitCallback([&](const std::string& cmd, std::int64_t) { first_commits.push_back(cmd); });

  first.Start();
  second.Start();
  third.Start();

  const int leader_idx = WaitForSingleLeader(nodes);
  assert(leader_idx >= 0);
  quorumdb::RaftNode *leader = nodes[leader_idx];

  // Test 5: Propose() on follower returns false
  quorumdb::RaftNode *follower = nodes[(leader_idx + 1) % 3];
  assert(!follower->Propose("should_fail"));

  // Test 1: Propose commands
  assert(leader->Propose("cmd1"));
  assert(leader->Propose("cmd2"));
  
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  
  // Test 1 & 2: Confirm logs match and commit index advances
  for (auto *node : nodes) {
    auto log = node->GetLog();
    assert(log.size() == 3); // sentinel + 2 cmds
    assert(log[1].command == "cmd1");
    assert(log[2].command == "cmd2");
    assert(node->GetCommitIndex() == 2);
  }

  // Test 3: Confirm callback fires in order
  assert(first_commits.size() == 2);
  assert(first_commits[0] == "cmd1");
  assert(first_commits[1] == "cmd2");

  first.Stop();
  second.Stop();
  third.Stop();
}

void TestCatchUpAfterPartition() {
  const std::vector<int> ports{ReserveLoopbackPort(), ReserveLoopbackPort(), ReserveLoopbackPort()};
  quorumdb::RaftNode first(1, PeersFor(1, ports), ports[0]);
  quorumdb::RaftNode second(2, PeersFor(2, ports), ports[1]);
  quorumdb::RaftNode third(3, PeersFor(3, ports), ports[2]);
  std::vector<quorumdb::RaftNode *> nodes{&first, &second, &third};
  first.Start();
  second.Start();
  third.Start();

  const int leader_idx = WaitForSingleLeader(nodes);
  assert(leader_idx >= 0);
  quorumdb::RaftNode *leader = nodes[leader_idx];

  assert(leader->Propose("cmd1"));
  WaitForCommitIndex(nodes, 1);

  // Stop one follower
  int follower_idx = (leader_idx + 1) % 3;
  nodes[follower_idx]->Stop();

  // Propose more entries (majority 2/3 succeeds)
  assert(leader->Propose("cmd2"));
  assert(leader->Propose("cmd3"));

  std::vector<quorumdb::RaftNode *> active_nodes;
  for (std::size_t i = 0; i < nodes.size(); ++i) {
    if (i != static_cast<std::size_t>(follower_idx)) {
      active_nodes.push_back(nodes[i]);
    }
  }
  WaitForCommitIndex(active_nodes, 3);

  // Restart stopped follower
  nodes[follower_idx]->Start();

  // Test 4: Confirm it catches up
  WaitForCommitIndex({nodes[follower_idx]}, 3);
  auto log = nodes[follower_idx]->GetLog();
  assert(log.size() == 4);
  assert(log[1].command == "cmd1");
  assert(log[2].command == "cmd2");
  assert(log[3].command == "cmd3");

  first.Stop();
  second.Stop();
  third.Stop();
}

}  // namespace

int main() {
  TestLogReplicationAndCommit();
  TestCatchUpAfterPartition();
  return 0;
}
