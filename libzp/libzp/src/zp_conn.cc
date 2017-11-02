/*
 * "Copyright [2016] qihoo"
 */
#include "libzp/src/zp_conn.h"

#include <algorithm>
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

ConnectionPool::ConnectionPool(size_t capacity) :
    capacity_(capacity), lru_head_(nullptr) {
  lru_head_.next = &lru_head_;
  lru_head_.prev = &lru_head_;
}

std::shared_ptr<ZpCli> ConnectionPool::GetConnection(
    const Node& node, uint64_t deadline, Status* sptr) {
  *sptr = Status::OK();
  slash::MutexLock l(&pool_mu_);
  auto it = conn_pool_.find(node);
  if (it != conn_pool_.end()) {
    if (it->second->cli->TryKeepalive()) {
      MoveToLRUHead(it->second);
      return it->second->cli;
    }
    RemoveFromLRU(it->second);
    conn_pool_.erase(it);
  }

  // Not found or timeout, create new one
  std::shared_ptr<ZpCli> zp_cli(new ZpCli(node));
  ZpCliItem* item = new ZpCliItem(zp_cli);
  *sptr = zp_cli->SetTimeout(deadline, TimeoutOptType::CONNECT);
  if (!sptr->ok()) {
    delete item;
    return nullptr;
  }
  *sptr = zp_cli->cli->Connect(node.ip, node.port);
  if (!sptr->ok()) {
    delete item;
    return nullptr;
  }

  // Remove rear item
  if (conn_pool_.size() == capacity_) {
    ZpCliItem* rear = lru_head_.prev;
    conn_pool_.erase(rear->cli->node);
    RemoveFromLRU(rear);
  }

  // Add new item
  conn_pool_.insert(std::make_pair(node, item));
  InsertLRU(item);
  return item->cli;
}

void ConnectionPool::RemoveConnection(std::shared_ptr<ZpCli> conn) {
  slash::MutexLock l(&pool_mu_);
  auto iter = conn_pool_.find(conn->node);
  assert(iter != conn_pool_.end());
  RemoveFromLRU(iter->second);
  conn_pool_.erase(iter);
}

// Only for meta node connection pool
std::shared_ptr<ZpCli> ConnectionPool::GetExistConnection() {
  Status s;
  std::map<Node, ZpCliItem*>::iterator first_conn;
  slash::MutexLock l(&pool_mu_);
  while (!conn_pool_.empty()) {
    first_conn = conn_pool_.begin();
    if (!first_conn->second->cli->TryKeepalive()) {
      // Expire connection
      RemoveFromLRU(first_conn->second);
      conn_pool_.erase(first_conn);
      continue;
    }
    return first_conn->second->cli;
  }
  return nullptr;
}

void ConnectionPool::MoveToLRUHead(ZpCliItem* item) {
  assert(item != nullptr &&
         item->next != nullptr &&
         item->prev != nullptr);
  item->prev->next = item->next;
  item->next->prev = item->prev;

  item->next = lru_head_.next;
  item->prev = &lru_head_;
  lru_head_.next->prev = item;
  lru_head_.next = item;
}

void ConnectionPool::InsertLRU(ZpCliItem* item) {
  assert(item != nullptr);
  item->next = lru_head_.next;
  item->prev = &lru_head_;

  lru_head_.next->prev = item;
  lru_head_.next = item;
}

void ConnectionPool::RemoveFromLRU(ZpCliItem* item) {
  assert(item != nullptr &&
         item->next != nullptr &&
         item->prev != nullptr);
  item->prev->next = item->next;
  item->next->prev = item->prev;
  delete item;
}

}  // namespace libzp
