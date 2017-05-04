/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */
#include "libzp/include/zp_conn.h"

#include <sys/time.h>
#include "pink/include/pink_cli.h"

namespace libzp {

const int kDataConnTimeout =  20000000;

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
    cli->set_connect_timeout(6000);
    assert(cli);
  }

ZpCli::~ZpCli() {
  cli->Close();
  delete cli;
}

bool ZpCli::CheckTimeout() {
  uint64_t now = NowMicros();
  if ((now - lastchecktime) > kDataConnTimeout) {
    return false;
  }
  lastchecktime = now;
  return true;
}

ConnectionPool::ConnectionPool() {
}

ConnectionPool::~ConnectionPool() {
  std::map<Node, ZpCli*>::iterator iter = conn_pool_.begin();
  while (iter != conn_pool_.end()) {
    delete iter->second;
    iter++;
  }
  conn_pool_.clear();
}

ZpCli* ConnectionPool::GetConnection(const Node& node) {
  std::map<Node, ZpCli*>::iterator it = conn_pool_.find(node);
  if (it != conn_pool_.end()) {
    if (it->second->CheckTimeout()) {
      return it->second;
    }
    delete it->second;
    conn_pool_.erase(it);
  }

  // Not found or timeout, create new one
  ZpCli* cli = new ZpCli(node);
  Status s = cli->cli->Connect(node.ip, node.port);
  if (s.ok()) {
    conn_pool_.insert(std::make_pair(node, cli));
    return cli;
  }
  delete cli;
  return NULL;
}

void ConnectionPool::RemoveConnection(ZpCli* conn) {
  Node node = conn->node;
  std::map<Node, ZpCli*>::iterator it = conn_pool_.find(node);
  if (it != conn_pool_.end()) {
    delete(it->second);
    conn_pool_.erase(it);
  }
}

ZpCli* ConnectionPool::GetExistConnection() {
  Status s;
  std::map<Node, ZpCli*>::iterator first;
  while (!conn_pool_.empty()) {
    first = conn_pool_.begin();
    if (!first->second->CheckTimeout()) {
      // Expire connection
      delete conn_pool_.begin()->second;
      conn_pool_.erase(conn_pool_.begin());
      continue;
    }
    return conn_pool_.begin()->second;
  }
  return NULL;
}

}  // namespace libzp
