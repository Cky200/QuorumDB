#include "raft/raft_server.h"

namespace quorumdb {

RaftServer::RaftServer(std::int32_t node_id,
                       const std::vector<std::pair<std::int32_t, std::string>>& peers,
                       int listen_port, QueryEngine* query_engine)
    : query_engine_(query_engine),
      state_machine_(std::make_unique<RaftStateMachine>(query_engine_)),
      raft_node_(std::make_unique<RaftNode>(node_id, peers, listen_port)) {
  
  raft_node_->SetOnCommitCallback([this](const std::string& serialized_command, std::int64_t log_index) {
    state_machine_->Apply(serialized_command, log_index);
  });
}

void RaftServer::Start() {
  raft_node_->Start();
}

void RaftServer::Stop() {
  raft_node_->Stop();
}

bool RaftServer::SubmitWrite(const RaftCommand& cmd) {
  if (raft_node_->GetState() != RaftState::LEADER) {
    return false;
  }
  return raft_node_->Propose(Serialize(cmd));
}

std::optional<Record> RaftServer::SelectById(std::int64_t id) {
  return query_engine_->SelectById(id);
}

std::vector<Record> RaftServer::SelectWhere(const std::function<bool(const Record&)>& predicate) {
  return query_engine_->SelectWhere(predicate);
}

RaftState RaftServer::GetState() const {
  return raft_node_->GetState();
}

std::int64_t RaftServer::GetCommitIndex() const {
  return raft_node_->GetCommitIndex();
}

}  // namespace quorumdb
