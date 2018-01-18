#include "libzp/include/zp_client.h"
#include "libzp/include/zp_cluster.h"

namespace libzp {

RawClient::RawClient(const Options& options)
    : cluster_(new Cluster(options)) {
}

RawClient::RawClient(const std::string& ip, const int port)
    : cluster_(new Cluster(ip, port)) {
}

Status RawClient::Set(const std::string& table_name,
                      const std::string& key,
                      const std::string& value,
                      int32_t ttl) {
  return cluster_->Set(table_name, key, value, ttl);
}

Status RawClient::Get(const std::string& table_name,
                      const std::string& key,
                      std::string* value) {
  return cluster_->Get(table_name, key, value);
}

Status RawClient::Mget(const std::string& table_name,
                       const std::vector<std::string>& keys,
                       std::map<std::string, std::string>* values) {
  return cluster_->Mget(table_name, keys, values);
}

Status RawClient::Delete(const std::string& table_name,
                         const std::string& key) {
  return cluster_->Delete(table_name, key);
}

Status RawClient::Aset(const std::string& table_name,
                       const std::string& key,
                       const std::string& value,
                       zp_completion_t complietion,
                       void* data,
                       int32_t ttl) {
  return cluster_->Aset(table_name, key, value, complietion, data, ttl);
}

Status RawClient::Adelete(const std::string& table_name,
                          const std::string& key,
                          zp_completion_t complietion,
                          void* data) {
  return cluster_->Adelete(table_name, key, complietion, data);
}

Status RawClient::Aget(const std::string& table_name,
                       const std::string& key,
                       zp_completion_t complietion,
                       void* data) {
  return cluster_->Aget(table_name, key, complietion, data);
}

Status RawClient::Amget(const std::string& table_name,
                        const std::vector<std::string>& keys,
                        zp_completion_t complietion,
                        void* data) {
  return cluster_->Amget(table_name, keys, complietion, data);
}

const std::string kTableTag = "###";

Status RawClient::PutRow(const std::string& table_name,
                         const std::string& primary_key,
                         const std::map<std::string, std::string>& columns) {
  if (columns.empty()) {
    return Status::InvalidArgument("Empty columns");
  }
  std::string hash_tag = kLBrace + kTableTag + primary_key + kRBrace;
  Cluster::Batch batch(hash_tag);
  for (auto& item : columns) {
    batch.Write(item.first, item.second);
  }
  return cluster_->WriteBatch(table_name, batch);
}

Status RawClient::GetRow(const std::string& table_name,
                         const std::string& primary_key,
                         const std::vector<std::string>& col_to_get,
                         std::map<std::string, std::string>* columns) {
  std::string hash_tag = kLBrace + kTableTag + primary_key + kRBrace;
  std::vector<std::string> keys;
  std::map<std::string, std::string> results;

  if (col_to_get.empty()) {
    Status s = cluster_->ListbyTag(table_name, hash_tag, &results);
    if (!s.ok()) {
      return s;
    }
  } else {
    // Build Mget request
    for (auto& c : col_to_get) {
      keys.emplace_back(hash_tag + c);
    }

    Status s = cluster_->Mget(table_name, keys, &results);
    if (!s.ok()) {
      return s;
    }
  }
  // Trim hash_tag from original key
  for (auto& kv : results) {
    std::string key = kv.first.substr(hash_tag.size());
    columns->insert(std::make_pair(key, kv.second));
  }
  return Status::OK();
}

Status RawClient::DeleteRow(const std::string& table_name,
                            const std::string& primary_key,
                            const std::vector<std::string>& col_to_delete) {
  std::string hash_tag = kLBrace + kTableTag + primary_key + kRBrace;
  if (col_to_delete.empty()) {
    return cluster_->DeletebyTag(table_name, hash_tag);
  }
  Cluster::Batch batch(hash_tag);
  for (auto& c : col_to_delete) {
    batch.Delete(c);
  }
  return cluster_->WriteBatch(table_name, batch);
}

}  // namespace libzp
