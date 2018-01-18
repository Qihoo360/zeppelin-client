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

class RawClient {
 public :
  RawClient(const Options& options);
  RawClient(const std::string& ip, const int port);

  virtual ~RawClient();

  // data cmd
  Status Set(const std::string& table_name,
             const std::string& key,
             const std::string& value,
             int32_t ttl = -1);
  Status Get(const std::string& table_name,
             const std::string& key,
             std::string* value);
  Status Mget(const std::string& table_name,
              const std::vector<std::string>& keys,
              std::map<std::string, std::string>* values);
  Status Delete(const std::string& table_name,
                const std::string& key);

  // async data cmd
  Status Aset(const std::string& table_name,
              const std::string& key,
              const std::string& value,
              zp_completion_t complietion,
              void* data,
              int32_t ttl = -1);
  Status Adelete(const std::string& table_name,
                 const std::string& key,
                 zp_completion_t complietion,
                 void* data);
  Status Aget(const std::string& table_name,
              const std::string& key,
              zp_completion_t complietion,
              void* data);
  Status Amget(const std::string& table_name,
               const std::vector<std::string>& keys,
               zp_completion_t complietion,
               void* data);

  Status PutRow(const std::string& table_name,
                const std::string& primary_key,
                const std::map<std::string, std::string>& columns);

  // Get all columns if col_to_delete is empty
  Status GetRow(const std::string& table_name,
                const std::string& primary_key,
                const std::vector<std::string>& col_to_get,
                std::map<std::string, std::string>* columns);

  // Delete all columns if col_to_delete is empty
  Status DeleteRow(const std::string& table_name,
                   const std::string& primary_key,
                   const std::vector<std::string>& col_to_delete);

 private :
  Cluster* cluster_;
};

class Client {
 public :
  Client(const Options& options, const std::string& table_name)
      : raw_client_(options),
        table_(table_name) {
  }
  Client(const std::string& ip, const int port, const std::string& table_name)
      : raw_client_(ip, port),
        table_(table_name) {
  }
  virtual ~Client() {}

  // data cmd
  Status Set(const std::string& key,
             const std::string& value,
             int32_t ttl = -1) {
    return raw_client_.Set(table_, key, value, ttl);
  }
  Status Get(const std::string& key,
             std::string* value) {
    return raw_client_.Get(table_, key, value);
  }
  Status Mget(const std::vector<std::string>& keys,
              std::map<std::string, std::string>* values) {
    return raw_client_.Mget(table_, keys, values);
  }
  Status Delete(const std::string& key) {
    return raw_client_.Delete(table_, key);
  }
  
  // async data cmd
  Status Aset(const std::string& key,
              const std::string& value,
              zp_completion_t complietion,
              void* data,
              int32_t ttl = -1) {
    return raw_client_.Aset(table_, key, value, complietion, data, ttl);
  }
  Status Adelete(const std::string& key,
                 zp_completion_t complietion,
                 void* data) {
    return raw_client_.Adelete(table_, key, complietion, data);
  }
  Status Aget(const std::string& key,
              zp_completion_t complietion,
              void* data) {
    return raw_client_.Aget(table_, key, complietion, data);
  }
  Status Amget(const std::vector<std::string>& keys,
               zp_completion_t complietion,
               void* data) {
    return raw_client_.Amget(table_, keys, complietion, data);
  }

  Status PutRow(const std::string& primary_key,
                const std::map<std::string, std::string>& columns) {
    return raw_client_.PutRow(table_, primary_key, columns);
  }

  // Get all columns if col_to_delete is empty
  Status GetRow(const std::string& primary_key,
                const std::vector<std::string>& col_to_get,
                std::map<std::string, std::string>* columns) {
    return raw_client_.GetRow(table_, primary_key, col_to_get, columns);
  }

  // Delete all columns if col_to_delete is empty
  Status DeleteRow(const std::string& primary_key,
                   const std::vector<std::string>& col_to_delete) {
    return raw_client_.DeleteRow(table_, primary_key, col_to_delete);
  }

 private :
  RawClient raw_client_;
  const std::string table_;
};

} // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_CLIENT_H_
