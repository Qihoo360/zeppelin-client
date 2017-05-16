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

}  // namespace libzp

