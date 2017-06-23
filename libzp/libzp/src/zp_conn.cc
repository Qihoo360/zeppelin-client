/*
 * "Copyright [2016] qihoo"
 */
#include "libzp/src/zp_conn.h"

#include <sys/time.h>
#include "slash/include/env.h"
#include "pink/include/pink_cli.h"

namespace libzp {

const int kConnKeepalive =  20000000;

static uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec)*1000000 +
    static_cast<uint64_t>(tv.tv_usec);
}

ZpCli::ZpCli(const Node& node)
: node(node),
  lastchecktime(NowMicros()) {
    cli = pink::NewPbCli();
    assert(cli);
  }

ZpCli::~ZpCli() {
  cli->Close();
  delete cli;
}

bool ZpCli::TryKeepalive() {
  uint64_t now = NowMicros();
  if ((now - lastchecktime) > kConnKeepalive) {
    return false;
  }
  lastchecktime = now;
  return true;
}

Status ZpCli::SetTimeout(uint64_t deadline, TimeoutOptType type) {
  if (!deadline) {
    return Status::OK(); // no limit
  }

  int timeout = deadline - slash::NowMicros() / 1000;
  if (timeout <= 0) {
    return Status::Timeout("timeout in SetTimeout");
  }

  switch (type) {
    case TimeoutOptType::CONNECT:
      cli->set_connect_timeout(timeout);
      break;
    case TimeoutOptType::SEND:
      cli->set_send_timeout(timeout);
      break;
    case TimeoutOptType::RECV:
      cli->set_recv_timeout(timeout);
      break;
    default:
      return Status::InvalidArgument("unknow TimeoutOptType");
  }
  return Status::OK();
}

ConnectionPool::ConnectionPool() {
  }

ConnectionPool::~ConnectionPool() {
  slash::MutexLock l(&pool_mu_);
  conn_pool_.clear();
}

std::shared_ptr<ZpCli> ConnectionPool::GetConnection(const Node& node,
    uint64_t deadline, Status* sptr) {
  *sptr = Status::OK();
  slash::MutexLock l(&pool_mu_);
  std::map<Node, std::shared_ptr<ZpCli>>::iterator it = conn_pool_.find(node);
  if (it != conn_pool_.end()) {
    if (it->second->TryKeepalive()) {
      return it->second;
    }
    conn_pool_.erase(it);
  }

  // Not found or timeout, create new one
  std::shared_ptr<ZpCli> cli(new ZpCli(node));
  *sptr = cli->SetTimeout(deadline, TimeoutOptType::CONNECT);
  if (sptr->ok()) {
    *sptr = cli->cli->Connect(node.ip, node.port);
  }
  if (sptr->ok()) {
    conn_pool_.insert(std::make_pair(node, cli));
    return cli;
  }
  return NULL;
}

void ConnectionPool::RemoveConnection(std::shared_ptr<ZpCli> conn) {
  slash::MutexLock l(&pool_mu_);
  Node node = conn->node;
  std::map<Node, std::shared_ptr<ZpCli>>::iterator it = conn_pool_.find(node);
  if (it != conn_pool_.end()) {
    conn_pool_.erase(it);
  }
}

std::shared_ptr<ZpCli> ConnectionPool::GetExistConnection() {
  Status s;
  std::map<Node, std::shared_ptr<ZpCli>>::iterator first;
  slash::MutexLock l(&pool_mu_);
  while (!conn_pool_.empty()) {
    first = conn_pool_.begin();
    if (!first->second->TryKeepalive()) {
      // Expire connection
      conn_pool_.erase(conn_pool_.begin());
      continue;
    }
    return conn_pool_.begin()->second;
  }
  return NULL;
}

}  // namespace libzp
