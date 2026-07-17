#pragma once

#include <string>

#include "rpc/rpc_message.h"

namespace quorumdb {

class RpcClient {
 public:
  bool SendMessage(const std::string &host, int port, const RpcMessage &message,
                   RpcMessage *out_reply, int timeout_ms) const;
};

}  // namespace quorumdb
