#pragma once

#include <cstdint>
#include <string>

#include "query/query_engine.h"

namespace quorumdb {

class RaftStateMachine {
 public:
  explicit RaftStateMachine(QueryEngine* engine);

  void Apply(const std::string& serialized_command, std::int64_t log_index);
  std::int64_t GetLastAppliedIndex() const;

 private:
  QueryEngine* engine_;
  std::int64_t last_applied_index_{0};
};

}  // namespace quorumdb
