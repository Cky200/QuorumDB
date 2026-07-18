#include "raft/state_machine.h"

#include "raft/raft_command.h"

namespace quorumdb {

RaftStateMachine::RaftStateMachine(QueryEngine* engine) : engine_(engine) {}

void RaftStateMachine::Apply(const std::string& serialized_command, std::int64_t log_index) {
  if (log_index <= last_applied_index_) {
    return;  // safe no-op
  }
  
  RaftCommand cmd = Deserialize(serialized_command);
  
  if (engine_) {
    switch (cmd.type) {
      case CommandType::INSERT:
        engine_->Insert(cmd.id, cmd.name, cmd.value);
        break;
      case CommandType::UPDATE_VALUE:
        engine_->UpdateValue(cmd.id, cmd.value);
        break;
      case CommandType::DELETE:
        engine_->DeleteById(cmd.id);
        break;
    }
  }
  
  last_applied_index_ = log_index;
}

std::int64_t RaftStateMachine::GetLastAppliedIndex() const {
  return last_applied_index_;
}

}  // namespace quorumdb
