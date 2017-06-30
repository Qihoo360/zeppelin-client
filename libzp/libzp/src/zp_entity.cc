/*
 * "Copyright [2016] qihoo"
 */
#include "libzp/src/zp_meta.pb.h"
#include "libzp/src/client.pb.h"
#include "slash/include/slash_string.h"
#include "libzp/include/zp_entity.h"

namespace libzp {
PartitionView::PartitionView(const client::PartitionState& state)
  : role(state.role()),
  repl_state(state.repl_state()),
  master(state.master().ip(), state.master().port()),
  file_num(state.sync_offset().filenum()),
  offset(state.sync_offset().offset()) {
    for (auto& s : state.slaves()) {
      slaves.push_back(Node(s.ip(), s.port()));
    }
  }

ServerState::ServerState(const client::CmdResponse::InfoServer& state)
  : epoch(state.epoch()),
  cur_meta(Node(state.cur_meta().ip(), state.cur_meta().port())),
  meta_renewing(state.meta_renewing()) {
    for (auto& s : state.table_names()) {
      table_names.push_back(s);
    }
  }

Partition::Partition(const ZPMeta::Partitions& partition_info)
  : master_(partition_info.master().ip(), partition_info.master().port()),
  id_(partition_info.id()) {
    for (int i = 0; i < partition_info.slaves_size(); i++) {
      slaves_.push_back(Node(partition_info.slaves(i).ip(),
            partition_info.slaves(i).port()));
    }
    id_ = partition_info.id();
    active_ = (partition_info.state() == ZPMeta::PState::ACTIVE);
  }

void Partition::DebugDump() const {
  std::cout << "  -partition: "<< id_ << std::endl;
  std::cout << "   -active: " << (active_ ? "true" : "false") << std::endl;
  std::cout << "   -master: " << master_.ip << " : "
    << master_.port << std::endl;
  for (auto& s : slaves_) {
    std::cout << "   -slave: " << s.ip << " : " << s.port << std::endl;
  }
}

void Partition::SetMaster(const Node& new_master) {
  master_.ip = new_master.ip;
  master_.port = new_master.port;
}

Table::Table()
  : partition_num_(0) {
  
  }

Table::Table(const ZPMeta::Table& table_info) {
  table_name_ = table_info.name();
  partition_num_ = table_info.partitions_size();
  ZPMeta::Partitions partition_info;
  for (int i = 0; i < table_info.partitions_size(); i++) {
    partition_info = table_info.partitions(i);
    partitions_.insert(std::make_pair(partition_info.id(),
           Partition(partition_info)));
  }
}

Table::~Table() {
}

const Partition* Table::GetPartition(const std::string& key) const {
  if (partitions_.empty()) {
    return NULL;
  }
  int par_num = std::hash<std::string>()(key) % partitions_.size();
  return GetPartitionById(par_num);
}

const Partition* Table::GetPartitionById(int par_num) const {
  if (partitions_.empty()) {
    return NULL;
  }
  auto iter = partitions_.find(par_num);
  if (iter != partitions_.end()) {
    return &(iter->second);
  } else {
    return NULL;
  }
}

void Table::DebugDump(int partition_id) const {
  std::cout << " -table name: "<< table_name_ <<std::endl;
  std::cout << " -partition num: "<< partition_num_ <<std::endl;
  if (partition_id != -1) {
    auto iter = partitions_.find(partition_id);
    if (iter != partitions_.end()) {
      iter->second.DebugDump();
    } else {
      std::cout << " -partition: "<< partition_id << ", not exist" <<std::endl;
    }
    return;
  }

  // Dump all
  for (auto& par : partitions_) {
    par.second.DebugDump();
  }
}

Status Table::UpdatePartitionMaster(const std::string& key,
    const Node& target) {
  if (partitions_.empty()) {
    return Status::InvalidArgument("no partition yet");
  }
  int par_num = std::hash<std::string>()(key) % partitions_.size();
  return UpdatePartitionMasterById(par_num, target);
}

Status Table::UpdatePartitionMasterById(int partition_id,
    const Node& target) {
  if (partitions_.empty()) {
    return Status::InvalidArgument("no partition yet");
  }

  auto iter = partitions_.find(partition_id);
  if (iter == partitions_.end()) {
    return Status::InvalidArgument("not fount partition");
  }
  partitions_.at(partition_id).SetMaster(target);
  return Status::OK();
}

void Table::GetAllMasters(std::set<Node>* nodes) const {
  for (auto& par : partitions_) {
    nodes->insert(par.second.master());
  }
}

void Table::GetAllNodes(std::set<Node>* nodes) const {
  for (auto& par : partitions_) {
    nodes->insert(par.second.master());
    for (auto& s : par.second.slaves()) {
      nodes->insert(s);
    }
  }
}


}  // namespace libzp
