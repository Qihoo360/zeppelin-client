/*
 * "Copyright [2016] qihoo"
 */

#ifndef CLIENT_INCLUDE_ZP_CONN_H_
#define CLIENT_INCLUDE_ZP_CONN_H_
#include <stdint.h>
#include <string>
#include <map>

#include "slash/include/slash_mutex.h"
#include "libzp/include/zp_table.h"

namespace pink {
  class PinkCli;
}

namespace libzp {

struct ZpCli {
  explicit ZpCli(const Node& node);
  ~ZpCli();
  bool CheckTimeout();

  Node node;
  pink::PinkCli* cli;
  slash::Mutex cli_mu;
  uint64_t lastchecktime;
};

class ConnectionPool {
 public :
  ConnectionPool();

  virtual ~ConnectionPool();

  ZpCli* GetConnection(const Node& node);
  void RemoveConnection(ZpCli* conn);
  ZpCli* GetExistConnection();

 private:
  std::map<Node, ZpCli*> conn_pool_;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_CONN_H_
