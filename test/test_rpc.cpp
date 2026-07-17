#include <cassert>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "rpc/rpc_client.h"
#include "rpc/rpc_server.h"

namespace {

quorumdb::RpcMessage EchoAsPong(const quorumdb::RpcMessage &message) {
  return {quorumdb::RpcMessageType::PONG, message.term, message.sender_id, message.payload};
}

void TestPingPong() {
  quorumdb::RpcServer server(0, EchoAsPong);
  server.Start();
  const int port = server.GetPort();
  quorumdb::RpcClient client;
  quorumdb::RpcMessage reply{};
  const quorumdb::RpcMessage request{quorumdb::RpcMessageType::PING, 7, 12, "hello"};
  assert(client.SendMessage("127.0.0.1", port, request, &reply, 500));
  assert(reply.type == quorumdb::RpcMessageType::PONG);
  assert(reply.term == 7);
  assert(reply.sender_id == 12);
  assert(reply.payload == "hello");
  server.Stop();
}

void TestConcurrentClients() {
  quorumdb::RpcServer server(0, EchoAsPong);
  server.Start();
  const int port = server.GetPort();
  constexpr int kClientCount = 12;
  std::vector<int> results(kClientCount, 0);
  std::vector<std::thread> clients;
  for (int index = 0; index < kClientCount; ++index) {
    clients.emplace_back([port, index, &results]() {
      quorumdb::RpcClient client;
      quorumdb::RpcMessage reply{};
      const std::string payload = "client-" + std::to_string(index);
      const quorumdb::RpcMessage request{quorumdb::RpcMessageType::PING, index, index, payload};
      results[index] = client.SendMessage("127.0.0.1", port, request, &reply, 1000) &&
                               reply.type == quorumdb::RpcMessageType::PONG && reply.term == index &&
                               reply.sender_id == index && reply.payload == payload;
    });
  }
  for (auto &client : clients) {
    client.join();
  }
  for (int result : results) {
    assert(result == 1);
  }
  server.Stop();
}

void TestUnreachableServerReturnsPromptly() {
  quorumdb::RpcServer server(0, EchoAsPong);
  server.Start();
  const int port = server.GetPort();
  server.Stop();
  quorumdb::RpcClient client;
  quorumdb::RpcMessage reply{};
  const auto start = std::chrono::steady_clock::now();
  const bool success = client.SendMessage("127.0.0.1", port,
                                          {quorumdb::RpcMessageType::PING, 0, 0, ""}, &reply, 200);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  assert(!success);
  assert(elapsed < std::chrono::seconds(2));
}

void TestMessageSerialization() {
  const quorumdb::RpcMessage original{quorumdb::RpcMessageType::APPEND_ENTRIES, 42, -9,
                                       std::string("raw\0payload", 11)};
  const std::string bytes = quorumdb::SerializeRpcMessage(original);
  quorumdb::RpcMessage decoded{};
  assert(quorumdb::DeserializeRpcMessage(bytes, &decoded));
  assert(decoded.type == original.type);
  assert(decoded.term == original.term);
  assert(decoded.sender_id == original.sender_id);
  assert(decoded.payload == original.payload);
}

}  // namespace

int main() {
  TestPingPong();
  TestConcurrentClients();
  TestUnreachableServerReturnsPromptly();
  TestMessageSerialization();
  return 0;
}
