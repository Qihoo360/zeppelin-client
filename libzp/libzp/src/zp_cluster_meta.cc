/*
 * "Copyright [2016] qihoo"
 */
#include "libzp/include/zp_cluster.h"

#include <sstream>
#include <queue>

#include "pink/include/pink_cli.h"
#include "libzp/src/zp_conn.h"
#include "libzp/src/zp_meta.pb.h"
#include "libzp/src/client.pb.h"

namespace libzp {

const int kMetaAttempt = 10;

uint64_t CalcDeadline(int timeout);

/*
 * deadline is 0 means no deadline
 */
Status Cluster::SubmitMetaCmd(ZPMeta::MetaCmd& req,
    ZPMeta::MetaCmdResponse *res, uint64_t deadline, int attempt) {
  Status s;
  res->Clear();
  std::shared_ptr<ZpCli> meta_cli = GetMetaConnection(deadline, &s);
  if (!meta_cli) {
    return s;
  }

  {
    slash::MutexLock l(&meta_cli->cli_mu);
    s = meta_cli->SetTimeout(deadline, TimeoutOptType::SEND);
    if (s.ok()) {
      s = meta_cli->cli->Send(&req);
    }
    if (s.ok()) {
      s = meta_cli->SetTimeout(deadline, TimeoutOptType::RECV);
    }
    if (s.ok()) {
      s = meta_cli->cli->Recv(res);
    }
  }

  if (!s.ok()) {
    meta_pool_->RemoveConnection(meta_cli);
    if (s.IsTimeout()) {
      return s;
    }
    if (attempt <= kMetaAttempt) {
      return SubmitMetaCmd(req, res, deadline, attempt + 1);
    }
  }
  return s;
}

Status Cluster::CreateTable(const std::string& table_name,
    const std::vector<std::vector<Node>>& distribution) {
  if (distribution.empty()) {
    return Status::InvalidArgument("Empty distribution");
  }

  std::vector<Node> nodes;
  std::vector<std::string> status;
  Status s = ListNode(&nodes, &status);
  if (!s.ok()) {
    return s;
  }
  // For check nodes aliveness
  std::set<Node> alive_nodes;
  assert(nodes.size() == status.size());
  for (size_t i = 0; i < nodes.size(); i++) {
    if (status[i] == "up") {
      alive_nodes.insert(nodes[i]);
    }
  }

  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::INIT);
  ZPMeta::Table* table = meta_cmd_->mutable_init()->mutable_table();
  table->set_name(table_name);
  size_t master_index = 0;
  for (size_t i = 0; i < distribution.size(); i++) {
    const std::vector<Node>& partition = distribution[i];
    master_index = (master_index + 1) % partition.size();
    ZPMeta::Partitions* p = table->add_partitions();
    p->set_id(i);
    p->set_state(ZPMeta::PState::ACTIVE);

    if (partition.empty()) {
      return Status::Corruption("Empty partition");
    }

    for (size_t j = 0; j < partition.size(); j++) {
      const Node& node = partition[j];
      if (alive_nodes.find(node) == alive_nodes.end()) {
        return Status::Corruption("Node " + node.ToString() + " is down");
      }
      ZPMeta::Node* n = j == master_index ?
        p->mutable_master() : p->add_slaves();
      n->set_ip(node.ip);
      n->set_port(node.port);
    }
  }

  // // Debug dump
  // for (int i = 0; i < table->partitions_size(); i++) {
  //   const ZPMeta::Partitions& p = table->partitions(i);
  //   const ZPMeta::Node& master = p.master();
  //   printf("Partition: %d, master %s:%d ", p.id(), master.ip().c_str(), master.port());
  //   for (int j = 0; j < p.slaves_size(); j++) {
  //     const ZPMeta::Node& slave = p.slaves(j);
  //     printf("slave%d: %s:%d ", j, slave.ip().c_str(), slave.port());
  //   }
  //   printf("\n");
  // }

  Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::DropTable(const std::string& table_name) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::DROPTABLE);
  ZPMeta::MetaCmd_DropTable* drop_table = meta_cmd_->mutable_drop_table();
  drop_table->set_name(table_name);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::Pull(const std::string& table) {
  return PullInternal(table, CalcDeadline(options_.op_timeout));
}

Status Cluster::FetchMetaInfo(const std::string& table, Table* table_meta) {
  slash::Status s = PullInternal(table, CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }
  slash::RWLock l(&meta_rw_, false);
  if (tables_.find(table) == tables_.end()) {
    return Status::InvalidArgument("table not found");
  }
  *table_meta = *(tables_[table]);
  return Status::OK();
}

Status Cluster::SetMaster(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::SETMASTER);
  ZPMeta::MetaCmd_SetMaster* set_master_cmd = meta_cmd_->mutable_set_master();
  ZPMeta::BasicCmdUnit* set_master_entity = set_master_cmd->mutable_basic();
  set_master_entity->set_name(table_name);
  set_master_entity->set_partition(partition_num);
  ZPMeta::Node* node = set_master_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::AddSlave(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::ADDSLAVE);
  ZPMeta::MetaCmd_AddSlave* add_slave_cmd = meta_cmd_->mutable_add_slave();
  ZPMeta::BasicCmdUnit* add_slave_entity = add_slave_cmd->mutable_basic();
  add_slave_entity->set_name(table_name);
  add_slave_entity->set_partition(partition_num);
  ZPMeta::Node* node = add_slave_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::RemoveSlave(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::REMOVESLAVE);
  ZPMeta::MetaCmd_RemoveSlave* remove_slave_cmd =
    meta_cmd_->mutable_remove_slave();
  ZPMeta::BasicCmdUnit* remove_slave_entity = remove_slave_cmd->mutable_basic();
  remove_slave_entity->set_name(table_name);
  remove_slave_entity->set_partition(partition_num);
  ZPMeta::Node* node = remove_slave_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::RemoveNodes(const std::vector<Node>& nodes) {
  // Check node load before send remove command to meta
  std::vector<std::string> tables;
  Status s = ListTable(&tables);
  if (!s.ok()) {
    return s;
  }
  for (auto& table : tables) {
    s = Pull(table);
    if (!s.ok()) {
      return s;
    }
    Table* table_ptr = tables_[table];
    if (table_ptr == nullptr) {
      return Status::Corruption("Can not find table_ptr");
    }
    std::map<Node, std::vector<const Partition*>> nodes_loads;
    table_ptr->GetNodesLoads(&nodes_loads);
    for (auto& node : nodes) {
      auto node_iter = nodes_loads.find(node);
      if (node_iter != nodes_loads.end() &&
          !node_iter->second.empty()) {
        return Status::Corruption("Can not remove nonempty node: " + node.ToString());
      }
    }
  }

  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::REMOVENODES);
  ZPMeta::MetaCmd_RemoveNodes* remove_nodes_cmd =
    meta_cmd_->mutable_remove_nodes();

  for (const auto& n : nodes) {
    ZPMeta::Node* node = remove_nodes_cmd->add_nodes();
    node->set_ip(n.ip);
    node->set_port(n.port);
  }

  printf("Remove nodes:\n");
  for (int i = 0; i < remove_nodes_cmd->nodes_size(); i++) {
    auto node = remove_nodes_cmd->nodes(i);
    printf(" --- %s:%d\n", node.ip().c_str(), node.port());
  }

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

namespace {

struct NodeWithLoad {
  Node node;
  std::vector<const Partition*> partitions;
};

int NodeLoad(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& n)  {
  auto iter = nodes_map.find(n.ip);
  if (iter != nodes_map.end()) {
    for (auto& nwl : iter->second) {
      if (nwl->node == n) {
        return nwl->partitions.size();
      }
    }
  }
  return 0;
}

std::shared_ptr<NodeWithLoad> FindNodeWithLoad(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& node) {
  assert(nodes_map.find(node.ip) != nodes_map.end());
  for (auto nwl : nodes_map.at(node.ip)) {
    if (nwl->node == node) {
      return nwl;
    }
  }
  return nullptr;
}

int ParCountOnHost(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& src_node, int par_id) {
  assert(nodes_map.find(src_node.ip) != nodes_map.end());
  const std::vector<std::shared_ptr<NodeWithLoad>>& n_vec = nodes_map.at(src_node.ip);
  int count = 0;
  for (auto& nwl : n_vec) {
    for (auto p : nwl->partitions) {
      if (p->id() == par_id) {
        count++;
      }
    }
  }
  return count;
}

bool ParExistOnNode(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& node, int par_id) {
  const auto nwl = FindNodeWithLoad(nodes_map, node);
  for (const auto p : nwl->partitions) {
    if (p->id() == par_id) {
      return true;
    }
  }
  return false;
}

const Partition* SelectOnePartition(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& src_node,
    const Node& dst_node) {
  const auto nwl = FindNodeWithLoad(nodes_map, src_node);
  if (nodes_map.size() == 1) {
    assert(nwl->partitions.size() > 0);
    for (const auto p : nwl->partitions) {
      if (ParExistOnNode(nodes_map, dst_node, p->id())) {
        continue;
      }
      return p;
    }
  } else if (nodes_map.size() == 2) {
    for (const auto p : nwl->partitions) {
      if (ParExistOnNode(nodes_map, dst_node, p->id())) {
        continue;
      }
      int load = ParCountOnHost(nodes_map, dst_node, p->id());
      if (load == 2) {
        continue;
      }
      return p;
    }
  } else {
    for (const auto p : nwl->partitions) {
      if (ParExistOnNode(nodes_map, dst_node, p->id())) {
        continue;
      }
      int load = ParCountOnHost(nodes_map, dst_node, p->id());
      if (load == 1) {
        continue;
      }
      return p;
    }
  }
  return nullptr;
}

bool IsValidDstNode(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& dst_node,
    int par_id) {
  if (ParExistOnNode(nodes_map, dst_node, par_id)) {
    return false;
  }
  if (nodes_map.size() == 1) {
    return true;
  } else if (nodes_map.size() == 2) {
    int load = ParCountOnHost(nodes_map, dst_node, par_id);
    if (load <= 1) {
      return true;
    }
  } else {
    int load = ParCountOnHost(nodes_map, dst_node, par_id);
    if (load == 0) {
      return true;
    }
  }
  return false;
}

}

Status Cluster::Expand(
    const std::string& table,
    const std::vector<Node>& new_nodes) {
  Status s;
  if (new_nodes.empty()) {
    return s;  // OK
  }
  s = PullInternal(table, CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }
  if (tables_.find(table) == tables_.end()) {
    return Status::InvalidArgument("table not fount");
  }

  auto cmp = [](std::shared_ptr<NodeWithLoad> left,
                std::shared_ptr<NodeWithLoad> right) {
    return left->partitions.size() < right->partitions.size();
  };
  std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>
    nodes_map;  // new nodes and origin nodes
  std::priority_queue<std::shared_ptr<NodeWithLoad>,
                      std::vector<std::shared_ptr<NodeWithLoad>>,
                      decltype(cmp)> src_nodes_queue(cmp);

  // Get origin and new nodes loads
  Table* table_ptr = tables_[table];
  std::map<Node, std::vector<const Partition*>> nodes_loads;
  table_ptr->GetNodesLoads(&nodes_loads);
  for (auto& nn : new_nodes) {
    if (nodes_loads.find(nn) != nodes_loads.end()) {
      continue;  // Exist node
    }
    std::shared_ptr<NodeWithLoad>
      item(new NodeWithLoad{nn, std::vector<const Partition*>{}});
    if (nodes_map.find(nn.ip) != nodes_map.end()) {
      nodes_map[nn.ip].push_back(item);
    } else {
      nodes_map.insert(
        std::make_pair(nn.ip, std::vector<std::shared_ptr<NodeWithLoad>>{item}));
    }
  }

  // Calculate average load and restriction
  int total_load = 0;
  for (auto& n : nodes_loads) {
    total_load += n.second.size();
    std::shared_ptr<NodeWithLoad> nwl(new NodeWithLoad{n.first, n.second});
    if (nodes_map.find(n.first.ip) != nodes_map.end()) {
      nodes_map[n.first.ip].push_back(nwl);
    } else {
      nodes_map.insert(std::make_pair(
        n.first.ip, std::vector<std::shared_ptr<NodeWithLoad>>{nwl}));
    }
    src_nodes_queue.push(nwl);
  }
  double average = static_cast<double>(total_load) /
    (nodes_loads.size() + new_nodes.size());
  int ceil_of_load = static_cast<int>(std::ceil(average));
  int floor_of_load = static_cast<int>(std::floor(average));

  // Debug: dump nodes_map
  for (auto& host : nodes_map) {
    printf("Host: %s\n", host.first.c_str());
    for (auto& node_with_load : host.second) {
      printf("\tNode: %s --- ", node_with_load->node.ToString().c_str());
      if (nodes_loads.find(node_with_load->node) == nodes_loads.end()) {
        printf("{}\n");
        continue;
      }
      printf("{");
      size_t i = 0;
      for (; i < node_with_load->partitions.size() - 1; i++) {
        printf("%d, ", node_with_load->partitions[i]->id());
      }
      printf("%d}\n", node_with_load->partitions[i]->id());
    }
  }

  // Init protobuf
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::MIGRATE);
  ZPMeta::MetaCmd_Migrate* migrate_cmd = meta_cmd_->mutable_migrate();
  migrate_cmd->set_origin_epoch(epoch_);

  for (auto& dst_node : new_nodes) {
    std::vector<std::shared_ptr<NodeWithLoad>> nodes_buffer;
    while (!src_nodes_queue.empty()) {
      auto nwl = src_nodes_queue.top();
      src_nodes_queue.pop();
      const Node& src_node = nwl->node;
      int load1 = NodeLoad(nodes_map, dst_node);
      int load2 = NodeLoad(nodes_map, src_node);
      if (load1 >= ceil_of_load) {
        src_nodes_queue.push(nwl);
        break;
      }
      if (load2 <= floor_of_load) {
        continue;
      }
      const Partition* p =
        SelectOnePartition(nodes_map, src_node, dst_node);
      if (p != nullptr) {
        // Build cmd proto
        auto cmd_unit = migrate_cmd->add_diff();
        cmd_unit->set_table(table);
        cmd_unit->set_partition(p->id());
        auto l = cmd_unit->mutable_left();
        l->set_ip(src_node.ip);
        l->set_port(src_node.port);
        auto r = cmd_unit->mutable_right();
        r->set_ip(dst_node.ip);
        r->set_port(dst_node.port);

        auto iter = nwl->partitions.begin();
        while (iter != nwl->partitions.end()) {
          if ((*iter)->id() == p->id()) {
            nwl->partitions.erase(iter);
            break;
          }
          iter++;
        }
        auto nwl1 = FindNodeWithLoad(nodes_map, dst_node);
        nwl1->partitions.push_back(p);

        src_nodes_queue.push(nwl);
      } else {
        nodes_buffer.push_back(nwl);
      }
    }
    for (auto nb : nodes_buffer) {
      src_nodes_queue.push(nb);
    }
  }

  // Dump migrate result
  printf("Move numbers: %d\n", migrate_cmd->diff_size());
  for (int i = 0; i < migrate_cmd->diff_size(); i++) {
    auto diff = migrate_cmd->diff(i);
    auto l = diff.left();
    auto r = diff.right();
    printf("Move table(%s) %s:%d - %d => %s:%d\n", diff.table().c_str(),
           l.ip().c_str(), l.port(), diff.partition(),
           r.ip().c_str(), r.port());
  }

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                    CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::Migrate(const std::string& table,
    const Node& src_node, int partition_id, const Node& dst_node) {
  Status s = PullInternal(table, CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }
  if (tables_.find(table) == tables_.end()) {
    return Status::InvalidArgument("table not fount");
  }

  Table* table_ptr = tables_[table];
  std::map<Node, std::vector<const Partition*>> nodes_loads;
  table_ptr->GetNodesLoads(&nodes_loads);

  if (nodes_loads.find(src_node) == nodes_loads.end() ||
      nodes_loads.find(dst_node) == nodes_loads.end()) {
    return Status::Corruption("Invalid node");
  }
  for (const auto p : nodes_loads[dst_node]) {
    if (p->id() == partition_id) {
      return Status::Corruption("Invalid Move");
    }
  }

  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::MIGRATE);
  ZPMeta::MetaCmd_Migrate* migrate_cmd = meta_cmd_->mutable_migrate();
  migrate_cmd->set_origin_epoch(epoch_);

  // Build cmd proto
  auto cmd_unit = migrate_cmd->add_diff();
  cmd_unit->set_table(table);
  cmd_unit->set_partition(partition_id);
  auto l = cmd_unit->mutable_left();
  l->set_ip(src_node.ip);
  l->set_port(src_node.port);
  auto r = cmd_unit->mutable_right();
  r->set_ip(dst_node.ip);
  r->set_port(dst_node.port);

  auto diff = migrate_cmd->diff(0);
  printf("Move %s:%d - %d => %s:%d\n",
         diff.left().ip().c_str(), diff.left().port(),
         diff.partition(),
         diff.right().ip().c_str(), diff.right().port());

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                    CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::Shrink(const std::string& table, const std::vector<Node>& deleting) {
  Status s;
  if (deleting.empty()) {
    return s; // OK
  }
  s = PullInternal(table, CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }
  if (tables_.find(table) == tables_.end()) {
    return Status::InvalidArgument("table not fount");
  }

  auto cmp = [](std::shared_ptr<NodeWithLoad> left,
                std::shared_ptr<NodeWithLoad> right) {
    return left->partitions.size() > right->partitions.size();
  };
  std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>
    nodes_map;  // new nodes and origin nodes
  std::priority_queue<std::shared_ptr<NodeWithLoad>,
                      std::vector<std::shared_ptr<NodeWithLoad>>,
                      decltype(cmp)> dst_nodes_queue(cmp);

  // Get origin and new nodes loads
  Table* table_ptr = tables_[table];
  std::map<Node, std::vector<const Partition*>> nodes_loads;
  std::map<Node, std::vector<const Partition*>> nodes_tobe_deleted;
  table_ptr->GetNodesLoads(&nodes_loads);

  // Checking
  for (auto& n : deleting) {
    auto iter = nodes_loads.find(n);
    if (iter == nodes_loads.end()) {
      return Status::NotFound(n.ToString());
    }
    nodes_tobe_deleted.insert(*iter);
    nodes_loads.erase(iter);
  }

  // Calculate average load and restriction
  for (auto& n : nodes_loads) {
    std::shared_ptr<NodeWithLoad> nwl(new NodeWithLoad{n.first, n.second});
    if (nodes_map.find(n.first.ip) != nodes_map.end()) {
      nodes_map[n.first.ip].push_back(nwl);
    } else {
      nodes_map.insert(std::make_pair(
        n.first.ip, std::vector<std::shared_ptr<NodeWithLoad>>{nwl}));
    }
    dst_nodes_queue.push(nwl);
  }

  // Debug: dump nodes_map
#if 0
  for (auto& host : nodes_map) {
    printf("Host: %s\n", host.first.c_str());
    for (auto& node_with_load : host.second) {
      printf("\tNode: %s --- ", node_with_load->node.ToString().c_str());
      if (nodes_loads.find(node_with_load->node) == nodes_loads.end()) {
        printf("{}\n");
        continue;
      }
      printf("{");
      size_t i = 0;
      for (; i < node_with_load->partitions.size() - 1; i++) {
        printf("%d, ", node_with_load->partitions[i]->id());
      }
      printf("%d}\n", node_with_load->partitions[i]->id());
    }
  }
#endif

  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::MIGRATE);
  ZPMeta::MetaCmd_Migrate* migrate_cmd = meta_cmd_->mutable_migrate();
  migrate_cmd->set_origin_epoch(epoch_);

  for (auto& n : nodes_tobe_deleted) {
    const Node& src_node = n.first;
    for (auto p : n.second) {
      std::vector<std::shared_ptr<NodeWithLoad>> nodes_buffer;
      while (true && !dst_nodes_queue.empty()) {
        auto nwl = dst_nodes_queue.top();
        const Node& dst_node = nwl->node;
        dst_nodes_queue.pop();
        if (IsValidDstNode(nodes_map, dst_node, p->id())) {
          // Build cmd proto
          auto cmd_unit = migrate_cmd->add_diff();
          cmd_unit->set_table(table);
          cmd_unit->set_partition(p->id());
          auto l = cmd_unit->mutable_left();
          l->set_ip(src_node.ip);
          l->set_port(src_node.port);
          auto r = cmd_unit->mutable_right();
          r->set_ip(dst_node.ip);
          r->set_port(dst_node.port);

          nwl->partitions.push_back(p);

          dst_nodes_queue.push(nwl);
          break;
        } else {
          nodes_buffer.push_back(nwl);
        }
      }
      for (auto nb : nodes_buffer) {
        dst_nodes_queue.push(nb);
      }
    }
  }

  // Dump migrate result
  for (int i = 0; i < migrate_cmd->diff_size(); i++) {
    auto diff = migrate_cmd->diff(i);
    auto l = diff.left();
    auto r = diff.right();
    printf("Move table(%s) %s:%d - %d => %s:%d\n",diff.table().c_str(),
           l.ip().c_str(), l.port(), diff.partition(),
           r.ip().c_str(), r.port());
  }
  if (migrate_cmd->diff_size() == 0) {
    return Status::Corruption("There is no reasonable way");
  }

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                    CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::ReplaceNode(const Node& ori_node, const Node& dst_node) {
  // Check dst_node
  std::vector<Node> nodes;
  std::vector<std::string> status;
  Status s = ListNode(&nodes, &status);
  if (!s.ok()) {
    return s;
  }
  assert(nodes.size() == status.size());
  size_t i = 0;
  for (; i < nodes.size(); i++) {
    if (nodes[i] == dst_node) {
      break;
    }
  }
  if (i >= nodes.size()) {
    return Status::Corruption("Cannot find dst_node: " + dst_node.ToString());
  } else if (status[i] != "up") {
    return Status::Corruption("dst_node is offline");
  }

  // Acquire origin nodes' infomation
  std::vector<std::string> tables;
  s = ListTable(&tables);
  if (!s.ok()) {
    return s;
  }
  for (auto& table : tables) {
    s = PullInternal(table, CalcDeadline(options_.op_timeout));
    if (!s.ok()) {
      return s;
    }
  }

  // Init protobuf
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::MIGRATE);
  ZPMeta::MetaCmd_Migrate* migrate_cmd = meta_cmd_->mutable_migrate();
  migrate_cmd->set_origin_epoch(epoch_);

  for (auto& table : tables) {
    Table* tbl_ptr = tables_[table];
    std::map<Node, std::vector<const Partition*>> nodes_loads;
    tbl_ptr->GetNodesLoads(&nodes_loads);
    auto item = nodes_loads.find(ori_node);
    if (item != nodes_loads.end()) {
      for (auto par : item->second) {
        auto cmd_unit = migrate_cmd->add_diff();
        cmd_unit->set_table(table);
        cmd_unit->set_partition(par->id());
        auto l = cmd_unit->mutable_left();
        l->set_ip(ori_node.ip);
        l->set_port(ori_node.port);
        auto r = cmd_unit->mutable_right();
        r->set_ip(dst_node.ip);
        r->set_port(dst_node.port);
      }
    }
  }

  // Dump migrate result
  printf("Move numbers: %d\n", migrate_cmd->diff_size());
  for (int i = 0; i < migrate_cmd->diff_size(); i++) {
    auto diff = migrate_cmd->diff(i);
    auto l = diff.left();
    auto r = diff.right();
    printf("Move table(%s) %s:%d - %d => %s:%d\n", diff.table().c_str(),
           l.ip().c_str(), l.port(), diff.partition(),
           r.ip().c_str(), r.port());
  }

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                    CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::CancelMigrate() {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::CANCELMIGRATE);

  Status s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                           CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::ListMeta(Node* master, std::vector<Node>* nodes) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::LISTMETA);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  ZPMeta::MetaNodes info = meta_res_->list_meta().nodes();
  master->ip = info.leader().ip();
  master->port = info.leader().port();
  for (int i = 0; i < info.followers_size(); i++) {
    Node slave_node;
    slave_node.ip = info.followers(i).ip();
    slave_node.port = info.followers(i).port();
    nodes->push_back(slave_node);
  }
  return Status::OK();
}

Status Cluster::MetaStatus(Node* leader, std::map<Node, std::string>* meta_status) {
  std::vector<libzp::Node> followers;
  Status ret = ListMeta(leader, &followers);
  if (!ret.ok()) {
    return ret;
  }

  (*meta_status)[*leader] = "Unknow";
  for (auto& follower : followers) {
    (*meta_status)[follower] = "Unknow";
  }

  int32_t version;
  int64_t begin_time;
  int32_t complete_proportion;
  std::string consistency_stautus;
  ret = MetaStatus(&version, &consistency_stautus,
                   &begin_time, &complete_proportion);
  if (!ret.ok()) {
    return ret;
  }

  std::istringstream input(consistency_stautus);
  for (std::string line; std::getline(input, line); ) {
    if (line.find(":") == std::string::npos) {
      continue;  // Skip header
    }
    std::string ip;
    int port;
    std::istringstream line_s(line);
    for (std::string word; std::getline(line_s, word, ' '); ) {
      if (word.find(":") != std::string::npos) {
        if (slash::ParseIpPortString(word, ip, port)) {
          Node n(ip, port - 100);
          (*meta_status)[n] = "Up";
          break;
        }
      }
    }
  }

  for (auto& item : *meta_status) {
    if (item.second != "Up") {
      item.second = "Down";  // Change 'Unknow' to 'Down'
    }
  }

  return ret;
}

Status Cluster::MetaStatus(std::string* consistency_stautus) {
  int32_t version;
  int64_t begin_time;
  int32_t complete_proportion;
  return MetaStatus(&version, consistency_stautus,
                    &begin_time, &complete_proportion);
}

Status Cluster::MetaStatus(int32_t* version,
                           std::string* consistency_stautus,
                           int64_t* begin_time,
                           int32_t* complete_proportion) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::METASTATUS);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  const ZPMeta::MetaCmdResponse_MetaStatus& mstatus = meta_res_->meta_status();
  *version = mstatus.version();
  *consistency_stautus = mstatus.consistency_stautus();

  const ZPMeta::MigrateStatus& migrate_status = mstatus.migrate_status();
  *begin_time = migrate_status.begin_time();
  *complete_proportion = migrate_status.complete_proportion();

  return Status::OK();
}

Status Cluster::ListNode(std::vector<Node>* nodes,
    std::vector<std::string>* status) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::LISTNODE);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  ZPMeta::Nodes info = meta_res_->list_node().nodes();
  for (int i = 0; i < info.nodes_size(); i++) {
    Node data_node;
    data_node.ip = info.nodes(i).node().ip();
    data_node.port = info.nodes(i).node().port();
    nodes->push_back(data_node);
    if (info.nodes(i).status() == 1) {
      status->push_back("down");
    } else {
      status->push_back("up");
    }
  }
  return Status::OK();
}

Status Cluster::ListTable(std::vector<std::string>* tables) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::LISTTABLE);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  ZPMeta::TableName info = meta_res_->list_table().tables();
  for (int i = 0; i < info.name_size(); i++) {
    tables->push_back(info.name(i));
  }
  return Status::OK();
}

}  // namespace libzp
