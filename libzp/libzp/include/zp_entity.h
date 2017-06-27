/*
 * "Copyright [2016] qihoo"
 */
#ifndef CLIENT_INCLUDE_ZP_ENTITY_H_
#define CLIENT_INCLUDE_ZP_ENTITY_H_

#include <string>
#include <map>
#include <set>
#include <vector>

#include "libzp/include/zp_option.h"

namespace client {
  class CmdResponse;
  class PartitionState;
  class CmdResponse_InfoServer;
}

namespace ZPMeta {
  class Table;
  class Partitions;
}

namespace libzp {

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
  std::vector<Node> slaves() const {
    return slaves_;
  }

  void SetMaster(const Node& new_master);

private:
  std::vector<Node> slaves_;
  Node master_;
  int id_;
  bool active_;
};

class Table {
 public:
  Table();
  explicit Table(const ZPMeta::Table& table_info);
  virtual ~Table();
  int partition_num() {
    return partition_num_;
  }

  const Partition* GetPartition(const std::string& key) const;
  const Partition* GetPartitionById(int id) const;
  Status UpdatePartitionMaster(const std::string& key, const Node& target);
  void GetAllMasters(std::set<Node>* nodes) const;
  void GetAllNodes(std::set<Node>* nodes) const;
  void DebugDump(int partition_id = -1) const;

 private:
  std::string table_name_;
  int partition_num_;
  std::map<int, Partition> partitions_;
};

struct PartitionView {
  std::string role;
  std::string repl_state;
  Node master;
  std::vector<Node> slaves;
  int32_t file_num;
  int64_t offset;
  PartitionView(const client::PartitionState& state);
};

struct ServerState {
  int64_t epoch;
  std::vector<std::string> table_names;
  Node cur_meta;
  bool meta_renewing;
  ServerState()
    : epoch(-1),
    meta_renewing(false) {
    }
  ServerState(const client::CmdResponse_InfoServer & state);
};

struct SpaceInfo {
  int64_t used;
  int64_t remain;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_ENTITY_H_
