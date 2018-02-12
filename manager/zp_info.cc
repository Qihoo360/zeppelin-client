// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <stdio.h>
#include <unistd.h>
#include <math.h>       // fabs
#include <ctime>

#include <fstream>
#include <vector>
#include <chrono>
#include <algorithm>    // std::sort

#include "libzp/include/zp_cluster.h"
#include "utils/json.h"

#define NONE                 "\e[0m"
#define BLACK                "\e[0;30m"
#define L_BLACK              "\e[1;30m"
#define RED                  "\e[0;31m"
#define L_RED                "\e[1;31m"
#define GREEN                "\e[0;32m"
#define L_GREEN              "\e[1;32m"
#define BROWN                "\e[0;33m"
#define YELLOW               "\e[1;33m"
#define BLUE                 "\e[0;34m"
#define L_BLUE               "\e[1;34m"
#define PURPLE               "\e[0;35m"
#define L_PURPLE             "\e[1;35m"
#define CYAN                 "\e[0;36m"
#define L_CYAN               "\e[1;36m"
#define GRAY                 "\e[0;37m"
#define WHITE                "\e[1;37m"

#define BOLD                 "\e[1m"
#define UNDERLINE            "\e[4m"
#define BLINK                "\e[5m"
#define REVERSE              "\e[7m"
#define HIDE                 "\e[8m"
#define CLEAR                "\e[2J"
#define CLRLINE              "\r\e[K"  // or "\e[1K\r"

static int kReplicaNum = 3;

libzp::Cluster* cluster;
std::map<std::string, std::map<int, libzp::PartitionView>> view_map;

const int64_t KB = 1024;
const int64_t MB = KB << 10;
const int64_t GB = MB << 10;
const int64_t TB = GB << 10;
const int64_t PB = TB << 10;

static std::string ToHuman(int64_t bytes) {
  char buf[100];
  if (bytes < KB) {
    sprintf(buf, "%ld bytes", bytes);
  } else if (bytes < MB) {
    sprintf(buf, "%0.3f KB", (double)bytes / KB);
  } else if (bytes < GB) {
    sprintf(buf, "%0.3f MB", (double)bytes / MB);
  } else if (bytes < TB) {
    sprintf(buf, "%0.3f GB", (double)bytes / GB);
  } else if (bytes < PB) {
    sprintf(buf, "%0.3f TB", (double)bytes / TB);
  } else {
    sprintf(buf, "%0.3f PB", (double)bytes / PB);
  }
  return std::string(buf);
}

struct CompDevFunc {
  CompDevFunc(double average) {
    average_ = average;
  }
  bool operator() (const std::pair<libzp::Node,
    std::vector<const libzp::Partition*>>& i, const std::pair<libzp::Node,
    std::vector<const libzp::Partition*>>& j) {
    int node_par_i = (int)i.second.size();
    int node_par_j = (int)j.second.size();
    double dev_i = (double)(node_par_i - average_) / average_;
    double dev_j = (double)(node_par_j - average_) / average_;
    return fabs(dev_i) > fabs(dev_j);
  }
  double average_;
};


static bool CompPercFunc(const std::pair<libzp::Node, libzp::SpaceInfo>& i,
    const std::pair<libzp::Node, libzp::SpaceInfo>& j) {
  double percentage_i = (double)i.second.used / (i.second.used + i.second.remain);
  double percentage_j = (double)j.second.used / (j.second.used + j.second.remain);
  return percentage_i > percentage_j;
}

std::map<libzp::Node, std::string> g_nodes;
void UpdateNodeStatus(std::vector<libzp::Node>& nodes,
    std::vector<std::string>& status) {
  g_nodes.clear();
  for (size_t i = 0; i < nodes.size(); i++) {
    g_nodes.insert(std::map<libzp::Node, std::string>::
        value_type(nodes[i], status[i]));
  }
}

uint32_t master_filenum = UINT32_MAX;
void InfoNode() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoNode");
  std::vector<libzp::Node> nodes;
  std::vector<std::string> status;
  slash::Status s = cluster->ListNode(&nodes, &status);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    return;
  }
  if (nodes.size() == 0) {
    printf(RED "No Nodes" NONE "\n");
    return;
  }
  std::map<std::string, std::vector<std::pair<int, bool>>> ip_nodes;
  for (size_t i = 0; i < nodes.size() && i < status.size(); ++i) {
    libzp::Node& node = nodes[i];
    std::string& ip = node.ip;
    bool node_status = status[i] == "up" ? true : false;
    int port = node.port;
    std::map<std::string, std::vector<std::pair<int, bool>>>::iterator iter =
        ip_nodes.find(ip);
    if (iter != ip_nodes.end()) {
      ip_nodes[ip].push_back(std::make_pair(port, node_status));
    } else {
      std::vector<std::pair<int, bool>> ports;
      ports.push_back(std::make_pair(port, node_status));
      ip_nodes[ip] = ports;
    }
  }
  printf(L_PURPLE "Hosts Count:\n" NONE);
  printf("  %lu\n", ip_nodes.size());
  printf(L_PURPLE "Nodes Count:\n" NONE);
  printf("  %lu\n", nodes.size());
  printf(L_PURPLE "Down Nodes:\n" NONE);
  std::map<std::string, std::vector<std::pair<int, bool>>>::iterator iter
    = ip_nodes.begin();
  for (; iter != ip_nodes.end(); ++iter) {
    const std::string& ip = iter->first;
    std::vector<std::pair<int, bool>>& ports = iter->second;
    for (size_t i = 0; i < ports.size(); ++i) {
      if (!ports[i].second){
        printf("%15s  %-5d", ip.c_str(), ports[i].first);
        printf(RED " [%s]" NONE "\n", "DOWN");
      }
    }
  }
}

void InfoNodeDetail() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoNodeDetail");
  std::vector<libzp::Node> nodes;
  std::vector<std::string> status;
  slash::Status s = cluster->ListNode(&nodes, &status);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    return;
  }
  if (nodes.size() == 0) {
    printf(RED "No Nodes" NONE "\n");
    return;
  }
  printf(REVERSE "%21s" NONE" " REVERSE "%10s" NONE" " REVERSE "%10s" NONE" "
      REVERSE "%21s" NONE" " REVERSE "%10s" NONE" "
      REVERSE "%30s" NONE"\n",
    "Node", "Status", "Epoch", "MetaNode", "Renewing", "Tables");
  for (size_t i = 0; i < nodes.size(); i++) {
    printf("%15s:%5d ", nodes[i].ip.c_str(), nodes[i].port);
    if (status[i] == "down") {
      printf(RED "%10s\n" NONE, "DOWN");
      continue;
    } else {
      printf(GREEN "%10s " NONE, "UP");
      libzp::Node node(nodes[i].ip, nodes[i].port);
      libzp::ServerState state;
      libzp::Status s = cluster->InfoServer(node, &state);
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
      }
      printf("%10ld %15s:%5d ", state.epoch, state.cur_meta.ip.c_str(),
          state.cur_meta.port);
      if (state.meta_renewing) {
        printf(RED "%10s ", "true");
      } else {
        printf("%10s ", "false");
      }
      for (size_t j = 0; j < state.table_names.size(); j++) {
        if (j < state.table_names.size() - 1) {
          printf("%s, ", state.table_names[j].c_str());
        } else {
          printf("%s", state.table_names[j].c_str());
        }
      }
      printf("\n");
    }
  }
}


void InfoMeta(mjson::Json* json) {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoMeta");
  std::map<libzp::Node, std::string> meta_status;
  libzp::Node leader;
  int32_t version;
  std::string consistency_stautus;
  slash::Status s = cluster->MetaStatus(&leader, &meta_status,
                                        &version, &consistency_stautus);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    json->AddStr("error", "true");
    return;
  }

  json->AddInt("count", meta_status.size());
  mjson::Json node_json(mjson::JsonType::kSingle);

  printf(L_PURPLE "Leader:\n" NONE);
  printf("  %s:%d ", leader.ip.c_str(), leader.port);
  printf(GREEN "Up\n" NONE);
  node_json.AddStr("node", leader.ip + ":" + std::to_string(leader.port));
  node_json.AddStr("status", "Up");
  json->AddJson("leader", node_json);


  mjson::Json followers_json(mjson::JsonType::kArray);
  printf(L_PURPLE "Followers:\n" NONE);
  for (auto iter = meta_status.begin(); iter != meta_status.end(); iter++) {
    node_json.Clear();
    if (iter->first == leader) {
      continue;
    }
    node_json.AddStr("node", iter->first.ip + ":" +
        std::to_string(iter->first.port));
    node_json.AddStr("status", iter->second);
    followers_json.PushJson(node_json);
    printf("  %s:%d ", iter->first.ip.c_str(), iter->first.port);
    if (iter->second == "Up") {
      printf(GREEN "%s\n" NONE, iter->second.c_str());
    } else {
      printf(RED "%s\n" NONE, iter->second.c_str());
    }
  }
  json->AddJson("followers", followers_json);

  int64_t migrate_begin_time = 0;
  int32_t complete_proportion = 0;
  s = cluster->MigrateStatus(&migrate_begin_time, &complete_proportion);
  json->AddInt("begin_time", migrate_begin_time);
  json->AddInt("complete_proportion", complete_proportion);
  if (!s.ok() && !s.IsNotFound()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    return;
  }
  printf(L_PURPLE "MigrateStatus\n" NONE);
  if (s.ok()) {
    printf("  begin_time: %ld\n", migrate_begin_time);
    printf("  complete_proportion: %d\n", complete_proportion);
  } else {
    printf("  No migrate task is running\n");
  }
}

void PartitionDetail(const std::string& table, int32_t id,
    const std::string& ip, int32_t port, bool is_master) {
  char buf[1024];
  snprintf(buf, sizeof(buf), "%s:%d-%s", ip.c_str(), port, table.c_str());
  auto it = view_map.find(buf);
  if (it == view_map.end()) {
    std::map<int, libzp::PartitionView> view;
    libzp::Node node(ip, port);

    // check if the node is down from g_nodes
    auto iter = g_nodes.find(node);
    if (iter != g_nodes.end()) {
      if (iter->second == "down") {
        printf("%15s:%5d" RED "%49s" NONE, ip.c_str(), port, "[DOWN]");
        if (is_master) {
          master_filenum = UINT32_MAX;
        }
        return;
      }
    }

    slash::Status s = cluster->InfoRepl(node, table, &view);
    if (!s.ok()) {
      printf("%15s:%5d" RED "%49s" NONE, ip.c_str(), port, "[Maybe DOWN]");
      if (is_master) {
        master_filenum = UINT32_MAX;
      }
      return;
    }
    view_map.insert(std::map<std::string,
        std::map<int, libzp::PartitionView> >::value_type(buf, view));

    it = view_map.find(buf);
  }
  auto iter = it->second.find(id);
  if (iter == it->second.end()) {
    return;
  }
  if (is_master) {
    master_filenum = iter->second.offset.filenum;
    printf("%15s:%5d[%s, %s, %10d %11ld]", ip.c_str(), port,
        iter->second.role.c_str(), iter->second.repl_state.c_str(),
        iter->second.offset.filenum, iter->second.offset.offset);
  } else {
    if (master_filenum != iter->second.offset.filenum) {
      printf("%15s:%5d[%s, %s, " RED "%10d" NONE " %11ld]", ip.c_str(), port,
          iter->second.role.c_str(), iter->second.repl_state.c_str(),
          iter->second.offset.filenum, iter->second.offset.offset);
    } else {
      printf("%15s:%5d[%s, %s, %10d %11ld]", ip.c_str(), port,
          iter->second.role.c_str(), iter->second.repl_state.c_str(),
          iter->second.offset.filenum, iter->second.offset.offset);
    }
  }
  printf(BLUE " |" NONE);
}

void InfoTable(const std::string& table, bool detail = false) {
  if (detail) {
    printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoTableDetail");
  } else {
    printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoTable");
  }
  std::vector<std::string> tables;
  slash::Status s = cluster->ListTable(&tables);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    return;
  }
  printf(L_PURPLE "Tables:\n" NONE);
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    printf("  %s", (*iter).c_str());
  }

  // update nodes status map
  std::vector<libzp::Node> nodes;
  std::vector<std::string> status;
  s = cluster->ListNode(&nodes, &status);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    return;
  }
  UpdateNodeStatus(nodes, status);

  printf("\n");
  printf(L_PURPLE "Detail:\n" NONE);
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    if (table != "" && table != *iter) {
      continue;
    }
    s = cluster->Pull(*iter);
    std::unordered_map<std::string, libzp::Table*> tables = cluster->tables();
    auto it = tables.find(*iter);
    if (it == tables.end()) {
      printf(RED "Failed: %s Not Found" NONE "\n", (*iter).c_str());
      continue;
    }
    printf(PURPLE "%s [%d]" NONE "\n", it->second->table_name().c_str(),
        it->second->partition_num());
    if (!detail) {
      printf(PURPLE "NON-active noodes details:" NONE "\n");
    }
    printf("%-5s %-10s %-70s %-140s\n",
        "Id", "State", "Master", "Slaves");
    std::map<int, libzp::Partition> partitions = it->second->partitions();
    for (auto& par : partitions) {
      if (!detail && par.second.state() == libzp::Partition::kActive) {
        continue;
      }
      printf("%-5d ", par.second.id());
      if (par.second.state() == libzp::Partition::kActive) {
        printf(GREEN "%-10s " NONE, "Active");
      } else if (par.second.state() == libzp::Partition::kStuck) {
        printf(RED "%-10s " NONE, "Stuck");
      } else if (par.second.state() == libzp::Partition::kSlowDown) {
        printf(YELLOW "%-10s " NONE, "SlowDown");
      }
      if (detail) {
        PartitionDetail(*iter, par.second.id(),
            par.second.master().ip,
            par.second.master().port, true);
        printf(" ");
        for (auto& s : par.second.slaves()) {
          PartitionDetail(*iter, par.second.id(), s.ip,
              s.port, false);
        }
      } else {
        printf("%-15s:%-5d" "%49s", par.second.master().ip.c_str(),
            par.second.master().port, " ");
        printf(BLUE " |" NONE);
        for (auto& s : par.second.slaves()) {
          printf("%-15s:%-5d", s.ip.c_str(), s.port);
          printf(BLUE " |" NONE);
        }
      }
      printf("\n");
    }
    std::map<libzp::Node, std::vector<const libzp::Partition*>> nodes_loads;
    it->second->GetNodesLoads(&nodes_loads);
    int node_size = nodes_loads.size();
    int partition_size = it->second->partition_num() * kReplicaNum;
    double ave_par_per_node = (double) partition_size / node_size;

    int counter = 0;
    if (!detail) {
      printf(PURPLE "Top Deviation of Average Partitions[%f]"
          "Per Node based on [%d] nodes:\n" NONE, ave_par_per_node, node_size);
      counter = 10;
    }

    std::vector<std::pair<libzp::Node, std::vector<const libzp::Partition*>>>
      nodes_loads_sort;
    for (auto& node : nodes_loads) {
      nodes_loads_sort.push_back(node);
    }
    sort(nodes_loads_sort.begin(), nodes_loads_sort.end(),
        CompDevFunc(ave_par_per_node));
    for (auto& node : nodes_loads_sort) {
      if (!detail && counter <= 0) {
        break;
      }
      printf("%s: ", node.first.ToString().c_str());
      int node_par_size = (int)node.second.size();
      double deviation = (double)(node_par_size - ave_par_per_node) /
        ave_par_per_node;
      printf("[%d] [%f] ", node_par_size, deviation);
      printf("[");
      const std::vector<const libzp::Partition*>& p_vec = node.second;
      const libzp::Partition* p;
      size_t i = 0;
      for (i = 0; i < p_vec.size() - 1; i++) {
        p = p_vec.at(i);
        if (p->master() == node.first) {
          printf(BROWN "%d*" NONE ",", p->id());
        } else {
          printf("%d,", p->id());
        }
      }
      p = p_vec.at(i);
      if (p->master() == node.first) {
        printf(BROWN "%d*" NONE "]\n", p->id());
      } else {
        printf("%d]\n", p->id());
      }
      counter--;
    }
  }
}

void InfoQuery(const std::string& table, mjson::Json* json) {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoQuery");
  std::vector<std::string> tables;
  slash::Status s = cluster->ListTable(&tables);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    json->AddStr("error", "true");
    return;
  }
  printf(REVERSE "%35s" NONE" " REVERSE "%10s" NONE" " REVERSE "%15s" NONE"\n",
      "Table", "QPS", "TotalQuery");
  mjson::Json query_json(mjson::JsonType::kArray);
  int64_t all_table_query = 0;
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    mjson::Json tmp_json(mjson::JsonType::kSingle);
    tmp_json.AddStr("name", *iter);
    if (table != "" && table != *iter) {
      continue;
    }
    int32_t qps = 0; int64_t total_query = 0;
    s = cluster->InfoQps(*iter, &qps, &total_query);
    if (!s.ok()) {
      printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
      tmp_json.AddStr("error", "true");
    }
    all_table_query += total_query;
    printf(PURPLE "%35s" NONE " %10d %15ld\n",
        (*iter).c_str(), qps, total_query);
    tmp_json.AddInt("qps", qps);
    tmp_json.AddInt("total_query", total_query);

    query_json.PushJson(tmp_json);
  }
  printf(PURPLE "Accumulated QPS: %-20ld\n" NONE, all_table_query);
  json->AddJson("detail", query_json);
}

void InfoSpace(const std::string& table, mjson::Json* json) {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoSpace");
  std::vector<std::string> tables;
  slash::Status s = cluster->ListTable(&tables);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    json->AddStr("error", "true");
    return;
  }
  mjson::Json table_json(mjson::JsonType::kArray);
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    if (table != "" && table != *iter) {
      continue;
    }
    mjson::Json tmp_json_1(mjson::JsonType::kSingle);
    tmp_json_1.AddStr("name", *iter);

    printf(PURPLE "%s" NONE"\n", (*iter).c_str());
    std::vector<std::pair<libzp::Node, libzp::SpaceInfo>> nodes;
    libzp::Status s = cluster->InfoSpace(*iter, &nodes);
    if (!s.ok()) {
      std::cout << "Failed: " << s.ToString() << std::endl;
      tmp_json_1.AddStr("error", "true");
    }
    // sort by percnetage
    std::sort(nodes.begin(), nodes.end(), CompPercFunc);
    printf(PURPLE "Top Percentage Used Details:\n" NONE);
    printf(REVERSE "%21s" NONE" " REVERSE "%15s" NONE " " REVERSE "%15s" NONE" "
        REVERSE "%15s" NONE REVERSE "%10s" NONE"\n",
      "Node", "Total","Used", "Remain", "Used %");

    mjson::Json node_json(mjson::JsonType::kArray);
    int counter = 10;
    for (auto it = nodes.begin(); it != nodes.end() && counter > 0; it++, counter--) {
      mjson::Json tmp_json_2(mjson::JsonType::kSingle);
      printf("%15s:%5d ", it->first.ip.c_str(), it->first.port);
      int64_t used = it->second.used;
      int64_t remain = it->second.remain;
      int64_t total = used + remain;
      double percentage = (double) used / total;
      printf("%15s %15s %15s %10f\n", ToHuman(total).c_str() ,ToHuman(used).c_str(),
        ToHuman(remain).c_str(), percentage);
      tmp_json_2.AddStr("node", it->first.ip + ":" +
                          std::to_string(it->first.port));
      tmp_json_2.AddStr("total", ToHuman(total));
      tmp_json_2.AddStr("used", ToHuman(used));
      tmp_json_2.AddStr("remain", ToHuman(remain));
      tmp_json_2.AddStr("percentage", std::to_string(percentage));
      node_json.PushJson(tmp_json_2);
    }
    tmp_json_1.AddJson("detail", node_json);
    table_json.PushJson(tmp_json_1);
  }
  json->AddJson("detail", table_json);
}

void Usage() {
  fprintf(stderr,
          "Usage: zp_info [-h host -p port -i info "
                    "-t table -j json_output_path "
                    "-c cluster_conf_path]\n"
          "\t-h     -- zeppelin meta ip(OPTIONAL default: 127.0.0.1) \n"
          "\t-p     -- zeppelin meta port(OPTIONAL default: 9221) \n"
          "\t-i     -- info field (OPTIONAL default: all)\n"
          "\t          INCLUDE [node, nodedetail, meta, table,"
                               " tabledetail, query, space, all, alldetail]\n"
          "\t-t     -- specific table(OPTIONAL default: all-table)\n"
          "\t-j     -- json output path(OPTIONAL default: ./info_json_result)\n"
          "\t          NOTICE: -t is disabled when -i is"
                       " [node, nodedetail, meta]\n"
          "\t-c     -- zeppelin cluster conf file path"
                       "(OPTIONAL, default: null)\n"
          "\t          NOTICE: if -c was set, it will mask -h & -p\n"
          "example: ./zp_info\n");
}

int main(int argc, char* argv[]) {
  libzp::Options option;
  std::string ip = "127.0.0.1";
  int port = 9221;
  std::string info = "all";
  std::string table = "";
  std::string json_path = "./info_json_result";
  std::string conf_path = "";
  char buf[1024];
  char c;
  while (-1 != (c = getopt(argc, argv, "h:p:i:t:j:c:"))) {
    switch (c) {
      case 'h':
        snprintf(buf, sizeof(buf), "%s", optarg);
        ip = std::string(buf);
        if (ip == "") {
          Usage();
          return 0;
        }
        break;
      case 'p':
        snprintf(buf, sizeof(buf), "%s", optarg);
        port = std::atoi(buf);
        break;
      case 'i':
        snprintf(buf, sizeof(buf), "%s", optarg);
        info = buf;
        break;
      case 't':
        snprintf(buf, sizeof(buf), "%s", optarg);
        table = buf;
        break;
      case 'j':
        snprintf(buf, sizeof(buf), "%s", optarg);
        json_path = buf;
        break;
      case 'c':
        snprintf(buf, sizeof(buf), "%s", optarg);
        conf_path = buf;
        break;
      default:
        Usage();
        return 0;
    }
  }

  if (conf_path != "") {
    std::ifstream in(conf_path);
    if (!in.is_open()) {
      printf("Open conf file failed\n");
      return false;
    }
    char buf[1024];
    std::string arg;
    while (!in.eof()) {
      in.getline(buf, sizeof(buf));
      arg = std::string(buf);
      if (arg.find("meta_addr") == std::string::npos) {
        continue;
      }
      std::string::size_type pos = arg.find_first_of(':');
      if (pos != std::string::npos) {
        arg = arg.substr(pos + 1);

        pos = arg.find_last_of(' ');
        if (pos != std::string::npos) {
          arg = arg.substr(pos + 1);
        }
      }
      break;
    }
    printf("Load Meta Addrs: %s\n", arg.c_str());
    std::string ip;
    int port;
    while (true) {
      std::string::size_type pos = arg.find(",");
      if (pos == std::string::npos) {
        std::string ip_port = arg;
        std::string::size_type t_pos = ip_port.find('/');
        if (t_pos != std::string::npos) {
          ip = ip_port.substr(0, t_pos);
          port = atoi(ip_port.substr(t_pos + 1).data());
          option.meta_addr.push_back(libzp::Node(ip, port));
        }
        break;
      }
      std::string ip_port = arg.substr(0, pos);
      std::string::size_type t_pos = ip_port.find('/');
      if (t_pos != std::string::npos) {
        ip = ip_port.substr(0, t_pos);
        port = atoi(ip_port.substr(t_pos + 1).data());
        option.meta_addr.push_back(libzp::Node(ip, port));
      }
      arg = arg.substr(pos + 1);
    }
  } else {
    option.meta_addr.push_back(libzp::Node(ip, port));
  }

  option.op_timeout = 5000;
  char info_time[64];
  std::time_t tt = std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::now());
  ctime_r(&tt, info_time);
  // remove last '\n' of info_time which was added by ctime_r
  info_time[strlen(info_time) - 1] = '\0';

  cluster = new libzp::Cluster(option);
  view_map.clear();

  if (info == "node") {
    InfoNode();
  } else if (info == "nodedetail") {
    InfoNodeDetail();
  } else if (info == "meta") {
    mjson::Json meta_json(mjson::JsonType::kSingle);
    InfoMeta(&meta_json);
  } else if (info == "table") {
    InfoTable(table, false);
  } else if (info == "tabledetail") {
    InfoTable(table, true);
  } else if (info == "query") {
    mjson::Json query_json(mjson::JsonType::kSingle);
    InfoQuery(table, &query_json);
  } else if (info == "space") {
    mjson::Json space_json(mjson::JsonType::kSingle);
    InfoSpace(table, &space_json);
  } else if (info == "all") {
    mjson::Json json(mjson::JsonType::kSingle);
    InfoNode();
    printf("\n");

    mjson::Json meta_json(mjson::JsonType::kSingle);
    InfoMeta(&meta_json);
    printf("\n");

    InfoTable(table, false);
    printf("\n");

    mjson::Json query_json(mjson::JsonType::kSingle);
    InfoQuery(table, &query_json);
    printf("\n");

    mjson::Json space_json(mjson::JsonType::kSingle);
    InfoSpace(table, &space_json);

    json.AddStr("info_time", info_time);
    json.AddInt("version", 2);

    json.AddJson("meta", meta_json);
    json.AddJson("query", query_json);
    json.AddJson("space", space_json);
    // only dump json to file in "-i all" mode
    std::string json_result = json.Encode();
    FILE* file = fopen(json_path.c_str(), "w");
    if (file != nullptr) {
      fwrite(json_result.data(), json_result.size(), 1, file);
      fclose(file);
    }
  } else if (info == "alldetail") {
    InfoNodeDetail();
    printf("\n");

    mjson::Json meta_json(mjson::JsonType::kSingle);
    InfoMeta(&meta_json);
    printf("\n");

    InfoTable(table, true);
    printf("\n");

    mjson::Json query_json(mjson::JsonType::kSingle);
    InfoQuery(table, &query_json);
    printf("\n");

    mjson::Json space_json(mjson::JsonType::kSingle);
    InfoSpace(table, &space_json);
  } else {
    Usage();
  }

  return 0;
}
