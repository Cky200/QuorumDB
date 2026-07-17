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

int LeaderCount(const std::vector<quorumdb::RaftNode *> &nodes) {
  int leaders = 0;
  for (const auto *node : nodes) {
    if (node->GetState() == quorumdb::RaftState::LEADER) {
      ++leaders;
    }
  }
  return leaders;
}

int WaitForSingleLeader(const std::vector<quorumdb::RaftNode *> &nodes) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (std::chrono::steady_clock::now() < deadline) {
    if (LeaderCount(nodes) == 1) {
      for (std::size_t index = 0; index < nodes.size(); ++index) {
        if (nodes[index]->GetState() == quorumdb::RaftState::LEADER) {
          return static_cast<int>(index);
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return -1;
}

void TestElectionAndFailover() {
  const std::vector<int> ports{ReserveLoopbackPort(), ReserveLoopbackPort(), ReserveLoopbackPort()};
  quorumdb::RaftNode first(1, PeersFor(1, ports), ports[0]);
  quorumdb::RaftNode second(2, PeersFor(2, ports), ports[1]);
  quorumdb::RaftNode third(3, PeersFor(3, ports), ports[2]);
  std::vector<quorumdb::RaftNode *> nodes{&first, &second, &third};
  first.Start();
  second.Start();
  third.Start();

  const int first_leader = WaitForSingleLeader(nodes);
  assert(first_leader >= 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  assert(LeaderCount(nodes) == 1);
  for (std::size_t index = 0; index < nodes.size(); ++index) {
    if (static_cast<int>(index) != first_leader) {
      assert(nodes[index]->GetState() == quorumdb::RaftState::FOLLOWER);
    }
  }
  const auto term = first.GetCurrentTerm();
  assert(second.GetCurrentTerm() == term);
  assert(third.GetCurrentTerm() == term);

  nodes[static_cast<std::size_t>(first_leader)]->Stop();
  std::vector<quorumdb::RaftNode *> survivors;
  for (std::size_t index = 0; index < nodes.size(); ++index) {
    if (static_cast<int>(index) != first_leader) {
      survivors.push_back(nodes[index]);
    }
  }
  assert(WaitForSingleLeader(survivors) >= 0);
  first.Stop();
  second.Stop();
  third.Stop();
}

void TestLoneNodeCannotWinThreeNodeCluster() {
  const std::vector<int> ports{ReserveLoopbackPort(), ReserveLoopbackPort(), ReserveLoopbackPort()};
  quorumdb::RaftNode lone_node(1, PeersFor(1, ports), ports[0]);
  lone_node.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(900));
  assert(lone_node.GetState() != quorumdb::RaftState::LEADER);
  assert(lone_node.GetState() == quorumdb::RaftState::CANDIDATE);
  lone_node.Stop();
}

}  // namespace

int main() {
  TestElectionAndFailover();
  TestLoneNodeCannotWinThreeNodeCluster();
  return 0;
}
