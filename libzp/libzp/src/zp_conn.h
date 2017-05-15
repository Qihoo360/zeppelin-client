/*
 * "Copyright [2016] qihoo"
 */

#ifndef CLIENT_INCLUDE_ZP_CONN_H_
#define CLIENT_INCLUDE_ZP_CONN_H_
#include <stdint.h>
#include <map>
#include <string>
#include <memory>

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

  std::shared_ptr<ZpCli> GetConnection(const Node& node);
  void RemoveConnection(std::shared_ptr<ZpCli> conn);
  std::shared_ptr<ZpCli> GetExistConnection();

 private:
  slash::Mutex pool_mu_;
  std::map<Node, std::shared_ptr<ZpCli>> conn_pool_;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_CONN_H_
