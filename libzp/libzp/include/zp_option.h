#ifndef CLIENT_INCLUDE_ZP_OPTION_H_
#define CLIENT_INCLUDE_ZP_OPTION_H_
#include <iostream>
#include "slash/include/slash_status.h"
#include "slash/include/slash_string.h"

namespace libzp {

using slash::Status;

struct Node {
  Node(const std::string& other_ip, int other_port)
    : ip(other_ip),
    port(other_port) {}
  Node()
    : port(0) {}

  std::string ip;
  int port;

  bool operator < (const Node& other) const {
    return (slash::IpPortString(ip, port) <
        slash::IpPortString(other.ip, other.port));
  }
  bool operator == (const Node& other) const {
    return (ip == other.ip && port == other.port);
  }
  friend std::ostream& operator<< (std::ostream& stream, const Node& node) {
    stream << node.ip << ":" << node.port;
    return stream;
  }
};

struct Options {
  std::vector<Node> meta_addr;
  Options() {
  }
};

struct Result {
  Status ret;
  const std::string* value;
  const std::map<std::string, std::string>* kvs;

  Result(Status r)
    : ret(r), value(NULL), kvs(NULL) {}

  Result(Status r, const std::string* v)
    : ret(r), value(v), kvs(NULL) {}
  
  Result(Status r, const std::map<std::string, std::string>* vs)
    : ret(r), value(NULL), kvs(vs) {}
};

typedef void (*zp_completion_t)(const struct Result& stat,
    const void* data);

}

#endif
