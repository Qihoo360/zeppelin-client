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
#include <ctime>

#include <fstream>
#include <vector>
#include <chrono>

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

libzp::Cluster* cluster;
std::map<std::string, std::map<int, libzp::PartitionView>> view_map;
bool health = true;

void CheckupMeta(mjson::Json* json) {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupMeta");
  std::map<libzp::Node, std::string> meta_status;
  libzp::Node leader;
  int32_t version;
  std::string consistency_stautus;
  slash::Status s = cluster->MetaStatus(&leader, &meta_status,
                                        &version, &consistency_stautus);
  if (!s.ok()) {
    printf(RED "CheckupMeta, MetaStatus Failed: %s" NONE "\n",
        s.ToString().c_str());
    json->AddStr("error", "true");
    health = false;
    return;
  }
  mjson::Json node_json(mjson::JsonType::kSingle);
  printf(L_PURPLE "Leader:\n" NONE);
  printf("  %s:%d ", leader.ip.c_str(), leader.port);
  printf(GREEN "Up\n" NONE);
  node_json.AddStr("node", leader.ip + ":" + std::to_string(leader.port));
  node_json.AddStr("status", "Up");
  json->AddJson("leader", node_json);

  printf(L_PURPLE "Followers:\n" NONE);
  bool pass = true;
  mjson::Json followers_json(mjson::JsonType::kArray);
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
      pass = false;
      health = false;
      printf(RED "%s\n" NONE, iter->second.c_str());
    }
  }
  json->AddJson("followers", followers_json);
  if (pass) {
    printf(GREEN " ...... PASSED\n" NONE);
    json->AddStr("result", "passed");
  } else {
    printf(RED "...... FAILED\n" NONE);
    json->AddStr("result", "failed");
  }
}

void CheckupNode(mjson::Json* json) {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupNode");
  std::vector<libzp::Node> nodes;
  std::vector<std::string> status;
  slash::Status s = cluster->ListNode(&nodes, &status);
  if (!s.ok()) {
    printf(RED "CheckupNode, ListNode Failed: %s" NONE "\n",
        s.ToString().c_str());
    json->AddStr("error", "true");
    health = false;
    return;
  }
  size_t alive_nodes_num = 0;
  for (size_t i = 0; i < status.size(); i++) {
    if (status[i] == "up") {
      alive_nodes_num++;
    }
  }

  json->AddInt("count", nodes.size());
  json->AddInt("up", alive_nodes_num);
  json->AddInt("down", nodes.size() - alive_nodes_num);

  printf("Nodes Count: %lu, Up: %lu, Down: ",
      nodes.size(), alive_nodes_num);
  if (alive_nodes_num == nodes.size()) {
    printf("0\n" GREEN " ...... PASSED\n" NONE);
    json->AddStr("result", "passed");
  } else {
    health = false;
    printf(BROWN "%lu\n" NONE RED " ...... FAILED\n" NONE,
        nodes.size() - alive_nodes_num);
    json->AddStr("result", "failed");
  }
}

void CheckupEpoch(mjson::Json* json) {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupEpoch");
  std::vector<std::string> tables;
  slash::Status s = cluster->ListTable(&tables);
  if (!s.ok()) {
    printf(RED "CheckupEpoch, ListTable Failed: %s" NONE "\n",
        s.ToString().c_str());
    json->AddStr("error", "true");
    health = false;
    return;
  }
  if (tables.empty()) {
    printf(RED "CheckupEpoch, No Table" NONE "\n");
    json->AddStr("error", "true");
    return;
  }
  s = cluster->Pull(tables[0]);
  if (!s.ok()) {
    printf(RED "CheckupEpoch, Pull Failed: %s" NONE "\n",
          s.ToString().c_str());
    json->AddStr("error", "true");
    return;
  }
  int meta_epoch = cluster->epoch();

  std::vector<libzp::Node> nodes;
  std::vector<std::string> status;
  s = cluster->ListNode(&nodes, &status);
  if (!s.ok()) {
    printf(RED "CheckupEpoch, ListNode Failed: %s" NONE "\n",
        s.ToString().c_str());
    json->AddStr("error", "true");
    return;
  }

  int dismatch_epoch_num = 0;
  for (size_t i = 0; i < nodes.size(); i++) {
    if (status[i] != "up") {
      dismatch_epoch_num++;
      continue;
    }

    libzp::ServerState state;
    libzp::Status s = cluster->InfoServer(nodes[i], &state);
    if (!s.ok()) {
      printf(RED "CheckupEpoch, InfoServer Failed: %s" NONE "\n",
          s.ToString().c_str());
      // just treat it as dismatch, we do not add 'error'->'true' to json
      dismatch_epoch_num++;
      continue;
    }
    if (state.epoch != meta_epoch) {
      dismatch_epoch_num++;
    }
  }

  printf("CurrentMetaEpoch: %d, DismatchNodesNum: ", meta_epoch);
  json->AddInt("epoch", meta_epoch);
  json->AddInt("dismatch_num", dismatch_epoch_num);
  if (dismatch_epoch_num) {
    health = false;
    printf(BROWN "%d\n" NONE RED " ...... FAILED" NONE "\n",
        dismatch_epoch_num);
    json->AddStr("result", "failed");
  } else {
    printf("0\n" GREEN "...... PASSED" NONE "\n");
    json->AddStr("result", "passed");
  }
}

void UpdateViewIfNeeded(const std::string& table,
    const std::string& ip, int32_t port) {
  char buf[1024];
  snprintf(buf, sizeof(buf), "%s:%d-%s", ip.c_str(), port, table.c_str());
  auto it = view_map.find(buf);
  if (it == view_map.end()) {
    std::map<int, libzp::PartitionView> view;
    libzp::Node node(ip, port);
    slash::Status s = cluster->InfoRepl(node, table, &view);
    if (!s.ok()) {
//      printf("%s:%d" RED "[Maybe Down]" NONE, ip.c_str(), port);
      return;
    }
    view_map.insert(std::map<std::string,
        std::map<int, libzp::PartitionView> >::value_type(buf, view));
  }
}


void CheckupTable(mjson::Json* json) {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupTable");
  /*
   * Get the table list
   */
  std::vector<std::string> tables;
  slash::Status s = cluster->ListTable(&tables);
  if (!s.ok()) {
    printf(RED "CheckupTable, ListTable Failed: %s" NONE "\n",
        s.ToString().c_str());
    json->AddStr("error", "true");
    return;
  }
  if (tables.empty()) {
    printf(RED "CheckupTable, No Table" NONE "\n");
    json->AddStr("error", "true");
    return;
  }
  printf("Table Count: %lu\n", tables.size());
  json->AddInt("count", tables.size());

  // Pull every table to get its repl & offset info
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    s = cluster->Pull(*iter);
    if (!s.ok()) {
      printf(RED "CheckupTable, Pull Failed: %s" NONE "\n",
            s.ToString().c_str());
      json->AddStr("error", "true");
      return;
    }
  }

  /*
   * Check up every table
   */
  std::unordered_map<std::string, libzp::Table*> tables_info =
    cluster->tables();
  mjson::Json table_json(mjson::JsonType::kArray);
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    printf(PURPLE " %s: " NONE, (*iter).c_str());

    auto it = tables_info.find(*iter);
    assert(it != tables_info.end());

    std::map<int, libzp::Partition> partitions = it->second->partitions();
    /*
     * check every partition of this table
     */

    // save partitions which repl-info between meta and node is dismatched
    std::vector<int> dismatch_repl_num;
    // save partitions which repl-node is DOWN
    std::vector<int> lack_alive_repl_num;
    // save partitions which offset is dismatched between its master & slaves
    std::vector<int> dismatch_offset_num;
    // save partitions which is stuck
    std::vector<int> stuck_num;
    // save partitions which is slowdown
    std::vector<int> slowdown_num;

    mjson::Json tmp_json(mjson::JsonType::kSingle);
    tmp_json.AddStr("name", *iter);

    for (auto& p : partitions) {
      /*
       * 1. check up master node first
       */
      UpdateViewIfNeeded(*iter, p.second.master().ip,
          p.second.master().port);

      char buf[1024];
      snprintf(buf, sizeof(buf), "%s:%d-%s", p.second.master().ip.c_str(),
          p.second.master().port, (*iter).c_str());
      auto it = view_map.find(buf);
      if (it == view_map.end()) {
        lack_alive_repl_num.push_back(p.second.id());
        continue;
      }

      auto p_from_view_iter = it->second.find(p.second.id());
      if (p_from_view_iter == it->second.end()) {
        dismatch_repl_num.push_back(p.second.id());
        continue;
      }

      // compare master
      if (p_from_view_iter->second.master.ip != p.second.master().ip ||
          p_from_view_iter->second.master.port != p.second.master().port) {
        dismatch_repl_num.push_back(p.second.id());
        continue;
      }

      // compare slaves
      bool slave_match = false;
      for (auto s_iter = p_from_view_iter->second.slaves.begin();
          s_iter != p_from_view_iter->second.slaves.end(); s_iter++) {
        slave_match = false;
        for (auto& s : p.second.slaves()) {
//          printf("%s:%d --- %s:%d\n",
//          s_iter->ip.c_str(), s_iter->port, s.ip.c_str(), s.port);
          if (s_iter->ip == s.ip &&
                s_iter->port == s.port) {
            slave_match = true;
            break;
          }
        }
        if (!slave_match) {
          dismatch_repl_num.push_back(p.second.id());
          break;
        }
      }
      if (!slave_match) {
        continue;
      }

      // get master offset, compare with slaves later
      uint32_t master_filenum = p_from_view_iter->second.offset.filenum;

      /*
       * 2. check up slaves
       */
      for (auto& slave : p.second.slaves()) {
        UpdateViewIfNeeded(*iter, slave.ip, slave.port);
        char buf[1024];
        snprintf(buf, sizeof(buf), "%s:%d-%s", slave.ip.c_str(),
            slave.port, (*iter).c_str());
        auto it = view_map.find(buf);
        if (it == view_map.end()) {
          lack_alive_repl_num.push_back(p.second.id());
          break;
        }

        auto p_from_view_iter = it->second.find(p.second.id());
        if (p_from_view_iter == it->second.end()) {
          dismatch_repl_num.push_back(p.second.id());
          break;
        }

        // compare master
        if (p_from_view_iter->second.master.ip != p.second.master().ip ||
            p_from_view_iter->second.master.port != p.second.master().port) {
          dismatch_repl_num.push_back(p.second.id());
          break;
        }

        // compare slaves
        bool slave_match = false;
        bool error_happend = false;
        for (auto s_iter = p_from_view_iter->second.slaves.begin();
            s_iter != p_from_view_iter->second.slaves.end(); s_iter++) {
          slave_match = false;
          error_happend = false;
          for (auto& s : p.second.slaves()) {
//            printf("%s:%d --- %s:%d\n",
//              s_iter->ip.c_str(), s_iter->port, s.ip.c_str(), s.port);
            if (s_iter->ip == s.ip &&
                  s_iter->port == s.port) {
              slave_match = true;
              break;
            }
          }
          if (!slave_match) {
            dismatch_repl_num.push_back(p.second.id());
            error_happend = true;
            break;
          }
          if (master_filenum != p_from_view_iter->second.offset.filenum) {
            dismatch_offset_num.push_back(p.second.id());
            error_happend = true;
            break;
          }
        }
        if (error_happend) {
          break;
        }
      }

      /*
       *  3. checkup partition state
       */
       if (p.second.state() == libzp::Partition::kStuck) {
         stuck_num.push_back(p.second.id());
       } else if (p.second.state() == libzp::Partition::kSlowDown) {
         slowdown_num.push_back(p.second.id());
       }
    }

    if (dismatch_repl_num.empty() && lack_alive_repl_num.empty() &&
          dismatch_offset_num.empty() && stuck_num.empty() &&
          slowdown_num.empty()) {
      printf(GREEN "...... PASSED\n" NONE);
      tmp_json.AddStr("result", "passed");
      table_json.PushJson(tmp_json);
      continue;
    }

    health = false;
    printf(RED "...... FAILED\n" NONE);
    tmp_json.AddStr("result", "failed");

    printf("--PartitionCount: %lu\n", partitions.size());
    tmp_json.AddInt("p-count", partitions.size());
    printf("----repl-inconsistent: ");
    if (dismatch_repl_num.empty()) {
      printf("0\n");
    } else {
      printf(BROWN "%lu " NONE "[", dismatch_repl_num.size());
      size_t i = 0;
      for (i = 0; i < dismatch_repl_num.size() - 1; i++) {
        printf(RED "%d, " NONE, dismatch_repl_num[i]);
      }
      if (i == dismatch_repl_num.size() - 1) {
        printf(RED "%d" NONE, dismatch_repl_num[i]);
      }
      printf("]\n");
    }
    tmp_json.AddInt("inconsistent", dismatch_repl_num.size());
    printf("----repl-incomplete: ");
    if (lack_alive_repl_num.empty()) {
      printf("0\n");
    } else {
      printf(BROWN "%lu " NONE "[", lack_alive_repl_num.size());
      size_t i = 0;
      for (i = 0; i < lack_alive_repl_num.size() - 1; i++) {
        printf(RED "%d, " NONE, lack_alive_repl_num[i]);
      }
      if (i == lack_alive_repl_num.size() - 1) {
        printf(RED "%d" NONE, lack_alive_repl_num[i]);
      }
      printf("]\n");
    }
    tmp_json.AddInt("incomplete", lack_alive_repl_num.size());
    printf("----repl-lagging: ");
    if (dismatch_offset_num.empty()) {
      printf("0\n");
    } else {
      printf(BROWN "%lu " NONE "[", dismatch_offset_num.size());
      size_t i = 0;
      for (i = 0; i < dismatch_offset_num.size() - 1; i++) {
        printf(RED "%d, " NONE, dismatch_offset_num[i]);
      }
      if (i == dismatch_offset_num.size() - 1) {
        printf(RED "%d" NONE, dismatch_offset_num[i]);
      }
      printf("]\n");
    }
    tmp_json.AddInt("lagging", dismatch_offset_num.size());
    printf("----repl-stuck: ");
    if (stuck_num.empty()) {
      printf("0\n");
    } else {
      printf(BROWN "%lu " NONE "[", stuck_num.size());
      size_t i = 0;
      for (i = 0; i < stuck_num.size() - 1; i++) {
        printf(RED "%d, " NONE, stuck_num[i]);
      }
      if (i == stuck_num.size() - 1) {
        printf(RED "%d" NONE, stuck_num[i]);
      }
      printf("]\n");
    }
    tmp_json.AddInt("stuck", stuck_num.size());
    printf("----repl-slowdown: ");
    if (slowdown_num.empty()) {
      printf("0\n");
    } else {
      printf(BROWN "%lu " NONE "[", slowdown_num.size());
      size_t i = 0;
      for (i = 0; i < slowdown_num.size() - 1; i++) {
        printf(RED "%d, " NONE, slowdown_num[i]);
      }
      if (i == slowdown_num.size() - 1) {
        printf(RED "%d" NONE, slowdown_num[i]);
      }
      printf("]\n");
    }
    tmp_json.AddInt("slowdown", slowdown_num.size());

    table_json.PushJson(tmp_json);
  }
  json->AddJson("detail", table_json);
}

void CheckupConclusion(mjson::Json* json) {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupConclusion");
  if (health) {
    json->AddStr("healthy", "true");
    printf(GREEN REVERSE "zeppelin HEALTHY\n" NONE);
  } else {
    printf(RED REVERSE "zeppelin UNHEALTHY\n" NONE);
    json->AddStr("healthy", "false");
  }
}

void Usage() {
  fprintf(stderr,
          "Usage: zp_checkup [-h host -p port -j json_output_path "
                    "-c cluster_conf_file_path]\n"
          "\t-h     -- zeppelin meta ip(OPTIONAL default: 127.0.0.1) \n"
          "\t-p     -- zeppelin meta port(OPTIONAL default: 9221) \n"
          "\t-j     -- json output path"
                       "(OPTIONAL default: ./checkup_json_result)\n"
          "\t-c     -- zeppelin cluster conf file path"
                       "(OPTIONAL, default: null)\n"
          "\t          NOTICE: if -c was set, it will mask -h & -p)\n"
          "example: ./zp_checkup\n");
}

int main(int argc, char* argv[]) {
  std::string ip = "127.0.0.1";
  int port = 9221;
  std::string json_path = "./checkup_json_result";
  std::string conf_path = "";
  char buf[1024];
  char c;
  while (-1 != (c = getopt(argc, argv, "h:p:j:c:"))) {
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

  libzp::Options option;
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

  char check_time[64];
  std::time_t tt = std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::now());
  ctime_r(&tt, check_time);
  // remove last '\n' of check_time which was added by ctime_r
  check_time[strlen(check_time) - 1] = '\0';

  cluster = new libzp::Cluster(option);
  view_map.clear();

  mjson::Json json(mjson::JsonType::kSingle);

  mjson::Json meta_json(mjson::JsonType::kSingle);
  CheckupMeta(&meta_json);

  mjson::Json node_json(mjson::JsonType::kSingle);
  CheckupNode(&node_json);

  mjson::Json epoch_json(mjson::JsonType::kSingle);
  CheckupEpoch(&epoch_json);

  mjson::Json table_json(mjson::JsonType::kSingle);
  CheckupTable(&table_json);

  mjson::Json conclusion_json(mjson::JsonType::kSingle);
  CheckupConclusion(&conclusion_json);

  json.AddStr("check_time", check_time);
  json.AddInt("version", 2);
  json.AddJson("conclusion", conclusion_json);
  json.AddJson("meta", meta_json);
  json.AddJson("node", node_json);
  json.AddJson("epoch", epoch_json);
  json.AddJson("table", table_json);
  std::string json_result = json.Encode();
  FILE* file = fopen(json_path.c_str(), "w");
  if (file != nullptr) {
    fwrite(json_result.data(), json_result.size(), 1, file);
    fclose(file);
  }

  return 0;
}
