/*
 * "Copyright [2016] qihoo"
 */
#ifndef CLIENT_INCLUDE_ZP_TABLE_H_
#define CLIENT_INCLUDE_ZP_TABLE_H_

#include <string>
#include <map>
#include <set>
#include <vector>

#include "libzp/include/zp_meta.pb.h"

namespace libzp {

struct Node {
  Node(const std::string& other_ip, int other_port);
  Node();

  std::string ip;
  int port;
  Node& operator = (const Node& other);
  bool operator < (const Node& other) const;
  bool operator == (const Node& other) const;
};

class Partition {
public:
  explicit Partition(const ZPMeta::Partitions& partition_info);
  void DebugDump() const;
  Node master() const {
    return master_;
  }
  int id() const {
    return id_;
  }

private:
  std::vector<Node> slaves_;
  Node master_;
  int id_;
};

class Table {
 public:
  explicit Table(const ZPMeta::Table& table_info);
  virtual ~Table();

  const Partition* GetPartition(const std::string& key) const;
  void GetAllMasters(std::set<Node>* nodes) const;
  void DebugDump(int partition_id) const;

 private:
  std::string table_name_;
  int partition_num_;
  std::map<int, Partition*> partitions_;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_TABLE_H_
