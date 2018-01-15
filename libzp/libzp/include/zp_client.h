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
  Client(const Options& options, const std::string& table);
  Client(const std::string& ip, const int port, const std::string& table);
  virtual ~Client();

  Status Connect();

  void SetTableName(const std::string& table_name) { table_.assign(table_name); }
  const std::string& table_name() { return table_; }

  // data cmd
  Status Set(const std::string& key, const std::string& value,
      int32_t ttl = -1);
  Status Get(const std::string& key, std::string* value);
  Status Mget(const std::vector<std::string>& keys,
      std::map<std::string, std::string>* values);
  Status Delete(const std::string& key);
  
  // async data cmd
  Status Aset(const std::string& key,
      const std::string& value, zp_completion_t complietion, void* data,
      int32_t ttl = -1);
  Status Adelete(const std::string& key,
      zp_completion_t complietion, void* data);
  Status Aget(const std::string& key,
      zp_completion_t complietion, void* data);
  Status Amget(const std::vector<std::string>& keys,
      zp_completion_t complietion, void* data);

  Status PutRow(
      const std::string primary_key,
      const std::map<std::string, std::string> columns);

  // Get all columns if col_to_delete is empty
  Status GetRow(
      const std::string& primary_key,
      const std::vector<std::string> col_to_get,
      std::map<std::string, std::string>* columns);

  // Delete all columns if col_to_delete is empty
  Status DeleteRow(
      const std::string primary_key,
      const std::vector<std::string> col_to_delete);

 private :
  Cluster* cluster_;
  std::string table_;
};


} // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_CLIENT_H_
