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
  offset(state.sync_offset().filenum(), state.sync_offset().offset()),
  fallback_time(0) {
    for (auto& s : state.slaves()) {
      slaves.push_back(Node(s.ip(), s.port()));
    }
    if (state.has_fallback()) {
      fallback_time = state.fallback().time();
      fallback_before = BinlogOffset(state.fallback().before().filenum(),
          state.fallback().before().offset());
      fallback_after = BinlogOffset(state.fallback().after().filenum(),
          state.fallback().after().offset());
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

    switch (partition_info.state()) {
      case ZPMeta::PState::ACTIVE:
        state_ = kActive;
        break;
      case ZPMeta::PState::STUCK:
        state_ = kStuck;
        break;
      case ZPMeta::PState::SLOWDOWN:
        state_ = kSlowDown;
        break;
      default:
        state_ = kUnknow;
        break;
    }
  }

void Partition::DebugDump() const {
  std::string state_str;
  switch (state_) {
    case kActive:
      state_str.assign("Active");
      break;
    case kStuck:
      state_str.assign("Stuck");
      break;
    case kSlowDown:
      state_str.assign("SlowDown");
      break;
    default:
      state_str.assign("Unknow");
      break;
  }
  printf("  -%d,\t state: %5s, master: %s:%d\t",
         id_, state_str.c_str(), master_.ip.c_str(), master_.port);
  for (auto& s : slaves_) {
    printf("slave: %s:%d\t", s.ip.c_str(), s.port);
  }
  printf("\n");
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

  // key := hash_tag
  std::string hash_tag(key);

  size_t l_brace = key.find(kLBrace);
  if (l_brace == 0) {
    // key := ...{hash_tag}...
    size_t r_brace = key.find(kRBrace, l_brace + 1);
    if (r_brace != std::string::npos) {
      hash_tag.assign(key.begin() + kLBrace.size(), key.begin() + r_brace);
    }
  }

  int par_num = std::hash<std::string>()(hash_tag) % partitions_.size();

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

void Table::GetNodesLoads(
      std::map<Node, std::vector<const Partition*>>* loads) const {
  for (auto& p : partitions_) {
    const Partition* p_ptr = &p.second;
    // Master
    auto mn = p.second.master();
    auto m_iter = loads->find(mn);
    if (m_iter == loads->end()) {
      loads->insert(std::make_pair(mn, std::vector<const Partition*>{p_ptr}));
    } else {
      m_iter->second.push_back(p_ptr);
    }
    // Slaves
    for (auto& s : p.second.slaves()) {
      auto s_iter = loads->find(s);
      if (s_iter == loads->end()) {
        loads->insert(std::make_pair(s, std::vector<const Partition*>{p_ptr}));
      } else {
        s_iter->second.push_back(p_ptr);
      }
    }
  }
}

std::ostream& operator<< (std::ostream& out, const BinlogOffset& bo) {
  out << bo.filenum << "_" << bo.offset;
  return out;
}

}  // namespace libzp
