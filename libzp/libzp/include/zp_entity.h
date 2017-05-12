/*
 * "Copyright [2016] qihoo"
 */
#ifndef CLIENT_INCLUDE_ZP_ENTITY_H_
#define CLIENT_INCLUDE_ZP_ENTITY_H_

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

  friend std::ostream& operator<< (std::ostream& stream, const Node& node) {
    stream << node.ip << ":" << node.port;
    return stream;
  }
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
  std::vector<Node> slaves() const {
    return slaves_;
  } 

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
  PartitionView(const client::PartitionState& state)
    : role(state.role()),
    repl_state(state.repl_state()),
    master(state.master().ip(), state.master().port()),
    file_num(state.sync_offset().filenum()),
    offset(state.sync_offset().offset()) {
      for (auto& s : state.slaves()) {
        slaves.push_back(Node(s.ip(), s.port()));
      }
  }
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

  ServerState(const client::CmdResponse::InfoServer& state)
    : epoch(state.epoch()),
    cur_meta(Node(state.cur_meta().ip(), state.cur_meta().port())),
    meta_renewing(state.meta_renewing()) {
      for (auto& s : state.table_names()) {
        table_names.push_back(s);
      }
    }
};

struct SpaceInfo {
  int64_t used;
  int64_t remain;
};

}  // namespace libzp
#endif  // CLIENT_INCLUDE_ZP_ENTITY_H_
