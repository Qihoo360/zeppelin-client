/*
 * "Copyright [2016] qihoo"
 */
#include "slash/include/slash_string.h"
#include "libzp/include/zp_table.h"
#include <iostream>

namespace libzp {

Node::Node(const std::string& other_ip, int other_port) :
  ip(other_ip),
  port(other_port) {
  }

Node::Node() :
  port(0) {
  }

Node& Node::operator = (const Node& other) {
  ip = other.ip;
  port = other.port;
  return *this;
}

bool Node::operator < (const Node& other) const {
  return (slash::IpPortString(ip, port) <
    slash::IpPortString(other.ip, other.port));
}

bool Node::operator == (const Node& other) const {
  return (ip == other.ip && port == other.port);
}

Partition::Partition(const ZPMeta::Partitions& partition_info)
  : master_(partition_info.master().ip(), partition_info.master().port()),
  id_(partition_info.id()) {
    for (int i = 0; i < partition_info.slaves_size(); i++) {
      slaves_.push_back(Node(partition_info.slaves(i).ip(),
            partition_info.slaves(i).port()));
    }
    id_ = partition_info.id();
  }

void Partition::DebugDump() const {
  std::cout << " --partition: "<< id_;
  std::cout << " --master: " << master_.ip << " : "
    << master_.port << std::endl;
  for (auto& s : slaves_) {
    std::cout << " --slave: " << s.ip << " : " << s.port << std::endl;
  }
}

Table::Table(const ZPMeta::Table& table_info) {
  table_name_ = table_info.name();
  partition_num_ = table_info.partitions_size();
  ZPMeta::Partitions partition_info;
  for (int i = 0; i < table_info.partitions_size(); i++) {
    partition_info = table_info.partitions(i);
    Partition* par = new Partition(partition_info);
    partitions_.insert(std::make_pair(partition_info.id(), par));
  }
}

Table::~Table() {
  for (auto& part : partitions_) {
    delete part.second;
  }
}

const Partition* Table::GetPartition(const std::string& key) {
  int par_num = std::hash<std::string>()(key) % partitions_.size();
  auto iter = partitions_.find(par_num);
  if (iter != partitions_.end()) {
    return iter->second;
  } else {
    return NULL;
  }
}

void Table::DebugDump() const {
  std::cout << " -table name: "<< table_name_ <<std::endl;
  std::cout << " -partition num: "<< partition_num_ <<std::endl;
  for (auto& par : partitions_) {
    par.second->DebugDump();
  }
}

void Table::GetAllMasters(std::set<Node>* nodes) {
  for (auto& par : partitions_) {
    nodes->insert(par.second->master());
  }
}

}  // namespace libzp
