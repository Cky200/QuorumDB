#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "query/query_engine.h"
#include "raft/raft_command.h"
#include "raft/raft_node.h"
#include "raft/state_machine.h"

namespace quorumdb {

class RaftServer {
 public:
  RaftServer(std::int32_t node_id, const std::vector<std::pair<std::int32_t, std::string>>& peers,
             int listen_port, QueryEngine* query_engine);

  void Start();
  void Stop();

  bool SubmitWrite(const RaftCommand& cmd);

  // Note: Reads do not go through Raft consensus for this stage as a known simplification.
  std::optional<Record> SelectById(std::int64_t id);
  std::vector<Record> SelectWhere(const std::function<bool(const Record&)>& predicate);

  RaftState GetState() const;
  std::int64_t GetCommitIndex() const;

 private:
  QueryEngine* query_engine_;
  std::unique_ptr<RaftStateMachine> state_machine_;
  std::unique_ptr<RaftNode> raft_node_;
};

}  // namespace quorumdb
