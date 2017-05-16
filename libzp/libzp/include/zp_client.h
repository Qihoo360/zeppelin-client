/*
 * "Copyright [2016] qihoo"
 */
#ifndef CLIENT_INCLUDE_ZP_CLIENT_H_
#define CLIENT_INCLUDE_ZP_CLIENT_H_

#include <map>
#include <string>
#include <vector>

#include "slash/include/slash_status.h"
#include "libzp/include/zp_option.h"

namespace libzp {

using slash::Status;

class Cluster;

class Client {
 public :
  explicit Client(const Options& options, const std::string& table);
  Client(const std::string& ip, const int port,
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
  
  // async data cmd
  Status Aset(const std::string& table, const std::string& key,
      const std::string& value, zp_completion_t complietion, void* data,
      int32_t ttl = -1);
  Status Adelete(const std::string& table, const std::string& key,
      zp_completion_t complietion, void* data);
  Status Aget(const std::string& table, const std::string& key,
      zp_completion_t complietion, void* data);
  Status Amget(const std::string& table, const std::vector<std::string>& keys,
      zp_completion_t complietion, void* data);

 private :
  Cluster* cluster_;
  const std::string table_;
};


} // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_CLIENT_H_
