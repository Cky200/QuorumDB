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

#include "raft/raft_server.h"
#include "storage/bplus_tree.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"

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

int WaitForSingleLeader(const std::vector<quorumdb::RaftServer *> &nodes) {
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

void WaitForCommitIndex(const std::vector<quorumdb::RaftServer *> &nodes, std::int64_t expected_commit_index) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
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

std::string TestFilename(const char *name, int node_id) {
  return std::string("test_raft_sm_") + name + "_" + std::to_string(node_id) + ".db";
}

void RemoveFile(const std::string &filename) { std::remove(filename.c_str()); }

struct ServerFixture {
  explicit ServerFixture(const std::string &name, std::int32_t node_id, const std::vector<int>& ports)
      : filename(TestFilename(name.c_str(), node_id)),
        disk_manager(filename),
        buffer_pool(16, &disk_manager),
        tree(&buffer_pool),
        query_engine(&tree),
        server(node_id, PeersFor(node_id, ports), ports[node_id - 1], &query_engine) {}

  ~ServerFixture() {
    RemoveFile(filename);
  }

  std::string filename;
  quorumdb::DiskManager disk_manager;
  quorumdb::BufferPoolManager buffer_pool;
  quorumdb::BPlusTree tree;
  quorumdb::QueryEngine query_engine;
  quorumdb::RaftServer server;
};

void TestReplicateAndQuery() {
  const std::vector<int> ports{ReserveLoopbackPort(), ReserveLoopbackPort(), ReserveLoopbackPort()};
  ServerFixture f1("req", 1, ports);
  ServerFixture f2("req", 2, ports);
  ServerFixture f3("req", 3, ports);
  
  std::vector<quorumdb::RaftServer *> nodes{&f1.server, &f2.server, &f3.server};

  f1.server.Start();
  f2.server.Start();
  f3.server.Start();

  const int leader_idx = WaitForSingleLeader(nodes);
  assert(leader_idx >= 0);
  quorumdb::RaftServer *leader = nodes[leader_idx];

  // Follower rejects write
  quorumdb::RaftServer *follower = nodes[(leader_idx + 1) % 3];
  assert(!follower->SubmitWrite({quorumdb::CommandType::INSERT, 1, "test_name", 42}));

  // Leader accepts write
  assert(leader->SubmitWrite({quorumdb::CommandType::INSERT, 1, "test_name", 42}));
  
  WaitForCommitIndex(nodes, 1);

  // Allow a tiny bit of time for state machine Apply to finish
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  for (auto *node : nodes) {
    auto rec = node->SelectById(1);
    assert(rec.has_value());
    assert(rec->id == 1);
    assert(rec->name == "test_name");
    assert(rec->value == 42);
  }

  f1.server.Stop();
  f2.server.Stop();
  f3.server.Stop();
}

void TestSequenceOfOperations() {
  const std::vector<int> ports{ReserveLoopbackPort(), ReserveLoopbackPort(), ReserveLoopbackPort()};
  ServerFixture f1("seq", 1, ports);
  ServerFixture f2("seq", 2, ports);
  ServerFixture f3("seq", 3, ports);
  
  std::vector<quorumdb::RaftServer *> nodes{&f1.server, &f2.server, &f3.server};

  f1.server.Start();
  f2.server.Start();
  f3.server.Start();

  const int leader_idx = WaitForSingleLeader(nodes);
  assert(leader_idx >= 0);
  quorumdb::RaftServer *leader = nodes[leader_idx];

  assert(leader->SubmitWrite({quorumdb::CommandType::INSERT, 10, "ten", 100}));
  assert(leader->SubmitWrite({quorumdb::CommandType::UPDATE_VALUE, 10, "", 999}));
  assert(leader->SubmitWrite({quorumdb::CommandType::INSERT, 20, "twenty", 200}));
  assert(leader->SubmitWrite({quorumdb::CommandType::DELETE, 10, "", 0}));
  
  WaitForCommitIndex(nodes, 4);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  for (auto *node : nodes) {
    auto rec10 = node->SelectById(10);
    assert(!rec10.has_value()); // deleted
    
    auto rec20 = node->SelectById(20);
    assert(rec20.has_value());
    assert(rec20->name == "twenty");
    assert(rec20->value == 200);
  }

  f1.server.Stop();
  f2.server.Stop();
  f3.server.Stop();
}

void TestCatchUpAfterPartition() {
  const std::vector<int> ports{ReserveLoopbackPort(), ReserveLoopbackPort(), ReserveLoopbackPort()};
  ServerFixture f1("part", 1, ports);
  ServerFixture f2("part", 2, ports);
  ServerFixture f3("part", 3, ports);
  
  std::vector<quorumdb::RaftServer *> nodes{&f1.server, &f2.server, &f3.server};

  f1.server.Start();
  f2.server.Start();
  f3.server.Start();

  const int leader_idx = WaitForSingleLeader(nodes);
  assert(leader_idx >= 0);
  quorumdb::RaftServer *leader = nodes[leader_idx];

  assert(leader->SubmitWrite({quorumdb::CommandType::INSERT, 1, "one", 10}));
  WaitForCommitIndex(nodes, 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int follower_idx = (leader_idx + 1) % 3;
  nodes[follower_idx]->Stop();

  assert(leader->SubmitWrite({quorumdb::CommandType::UPDATE_VALUE, 1, "", 100}));
  assert(leader->SubmitWrite({quorumdb::CommandType::INSERT, 2, "two", 20}));

  std::vector<quorumdb::RaftServer *> active_nodes;
  for (std::size_t i = 0; i < nodes.size(); ++i) {
    if (i != static_cast<std::size_t>(follower_idx)) {
      active_nodes.push_back(nodes[i]);
    }
  }
  WaitForCommitIndex(active_nodes, 3);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  nodes[follower_idx]->Start();
  
  WaitForCommitIndex({nodes[follower_idx]}, 3);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto rec1 = nodes[follower_idx]->SelectById(1);
  assert(rec1.has_value());
  assert(rec1->value == 100);

  auto rec2 = nodes[follower_idx]->SelectById(2);
  assert(rec2.has_value());
  assert(rec2->name == "two");
  assert(rec2->value == 20);

  f1.server.Stop();
  f2.server.Stop();
  f3.server.Stop();
}

}  // namespace

int main() {
  TestReplicateAndQuery();
  TestSequenceOfOperations();
  TestCatchUpAfterPartition();
  return 0;
}
