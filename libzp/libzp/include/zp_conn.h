/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */

#ifndef CLIENT_INCLUDE_ZP_CONN_H_
#define CLIENT_INCLUDE_ZP_CONN_H_
#include <stdint.h>
#include <string>
#include <map>

#include "libzp/include/zp_meta.pb.h"
#include "libzp/include/zp_table.h"

namespace pink {
  class PinkCli;
}

namespace libzp {

struct ZpCli {
 public:
  explicit ZpCli(const Node& node);
  ~ZpCli();
  bool CheckTimeout();

  Node node;
  pink::PinkCli* cli;
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
