/*
 * "Copyright [2016] qihoo"
 */
#ifndef CLIENT_INCLUDE_ZP_CLIENT_H_
#define CLIENT_INCLUDE_ZP_CLIENT_H_

#include <map>
#include <string>
#include <vector>

#include "slash/include/slash_status.h"

namespace libzp {

using slash::Status;

class Cluster;

class Client {
 public :

  explicit Client(const std::string& ip, const int port,
      const std::string& table);
  virtual ~Client();
  Status Connect();

  // data cmd
  Status Set(const std::string& key, const std::string& value,
      int32_t ttl = -1);
  Status Get(const std::string& key, std::string* value);
  Status Mget(const std::vector<std::string>& keys,
      std::map<std::string, std::string>* values);
  Status Delete(const std::string& key);


 private :
  Cluster* cluster_;
  const std::string table_;
};


} // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_CLIENT_H_
