#include "libzp/include/zp_client.h"
#include "libzp/include/zp_cluster.h"

namespace libzp {

Client::Client(const Options& options, const std::string& table)
  :cluster_(new Cluster(options)),
  table_(table){
  }

Client::Client(const std::string& ip, const int port, const std::string& table)
  : cluster_(new Cluster(ip, port)),
  table_(table) {
  }

Client::~Client() {
  delete cluster_;
}

Status Client::Connect() {
  Status s = cluster_->Connect();
  if (!s.ok()) {
    return s;
  }
  s = cluster_->Pull(table_);
  return s;
}

Status Client::Set(const std::string& key, const std::string& value,
    int32_t ttl) {
  return cluster_->Set(table_, key, value, ttl);
}

Status Client::Get(const std::string& key, std::string* value) {
  return cluster_->Get(table_, key, value);
}

Status Client::Mget(const std::vector<std::string>& keys,
    std::map<std::string, std::string>* values) {
  return cluster_->Mget(table_, keys, values);
}

Status Client::Delete(const std::string& key) {
  return cluster_->Delete(table_, key);
}

Status Client::Aset(const std::string& key,
    const std::string& value, zp_completion_t complietion, void* data,
    int32_t ttl) {
  return cluster_->Aset(table_, key, value, complietion, data, ttl);
}

Status Client::Adelete(const std::string& key,
    zp_completion_t complietion, void* data) {
  return cluster_->Adelete(table_, key, complietion, data);
}

Status Client::Aget(const std::string& key,
    zp_completion_t complietion, void* data) {
  return cluster_->Aget(table_, key, complietion, data);
}

Status Client::Amget(const std::vector<std::string>& keys,
    zp_completion_t complietion, void* data) {
  return cluster_->Amget(table_, keys, complietion, data);
}

const std::string kTableTag = "###";

Status Client::PutRow(
    const std::string primary_key,
    const std::map<std::string, std::string> columns) {
  if (columns.empty()) {
    return Status::InvalidArgument("Empty columns");
  }
  std::string hash_tag = kLBrace + kTableTag + primary_key + kRBrace;
  Cluster::Batch batch(hash_tag);
  for (auto& item : columns) {
    batch.Write(item.first, item.second);
  }
  return cluster_->WriteBatch(table_, batch);
}

Status Client::GetRow(
    const std::string& primary_key,
    const std::vector<std::string> col_to_get,
    std::map<std::string, std::string>* columns) {
  std::string hash_tag = kLBrace + kTableTag + primary_key + kRBrace;
  std::vector<std::string> keys;
  std::map<std::string, std::string> results;

  if (col_to_get.empty()) {
    Status s = cluster_->ListbyTag(table_, hash_tag, &results);
    if (!s.ok()) {
      return s;
    }
  } else {
    // Build Mget request
    for (auto& c : col_to_get) {
      keys.emplace_back(hash_tag + c);
    }

    Status s = cluster_->Mget(table_, keys, &results);
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

Status Client::DeleteRow(
    const std::string primary_key,
    const std::vector<std::string> col_to_delete) {
  std::string hash_tag = kLBrace + kTableTag + primary_key + kRBrace;
  if (col_to_delete.empty()) {
    return cluster_->DeletebyTag(table_, hash_tag);
  }
  Cluster::Batch batch(hash_tag);
  for (auto& c : col_to_delete) {
    batch.Delete(c);
  }
  return cluster_->WriteBatch(table_, batch);
}

}  // namespace libzp
