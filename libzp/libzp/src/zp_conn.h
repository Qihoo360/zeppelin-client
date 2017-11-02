/*
 * "Copyright [2016] qihoo"
 */

#ifndef CLIENT_INCLUDE_ZP_CONN_H_
#define CLIENT_INCLUDE_ZP_CONN_H_
#include <stdint.h>
#include <map>
#include <list>
#include <string>
#include <memory>

#include "slash/include/slash_mutex.h"
#include "libzp/include/zp_entity.h"

namespace pink {
  class PinkCli;
}

namespace libzp {

enum TimeoutOptType {
  CONNECT = 0,
  SEND,
  RECV,
};

struct ZpCli {
  explicit ZpCli(const Node& node);
  ~ZpCli();
  bool TryKeepalive();
  Status SetTimeout(uint64_t deadline, TimeoutOptType type);

  Node node;
  pink::PinkCli* cli;
  slash::Mutex cli_mu;
  uint64_t lastchecktime;
};

class ConnectionPool {
 public :
  explicit ConnectionPool(size_t capacity);
  std::shared_ptr<ZpCli> GetConnection(
    const Node& node, uint64_t deadline, Status* sptr);
  void RemoveConnection(std::shared_ptr<ZpCli> conn);
  std::shared_ptr<ZpCli> GetExistConnection();

 private:
  struct ZpCliItem {
    explicit ZpCliItem(ZpCli* zp_cli)
        : cli(zp_cli),
          next(nullptr),
          prev(nullptr) {
    }
    std::shared_ptr<ZpCli> cli;
    ZpCliItem* next;
    ZpCliItem* prev;
  };

  slash::Mutex pool_mu_;
  std::map<Node, ZpCliItem*> conn_pool_;
  size_t capacity_;
  ZpCliItem lru_head_;

  void MoveToLRUHead(ZpCliItem* item);
  void InsertLRU(ZpCliItem* item);
  void RemoveFromLRU(ZpCliItem* item);
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_CONN_H_
