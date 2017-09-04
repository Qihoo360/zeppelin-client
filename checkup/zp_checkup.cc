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

#include <vector>

#include "libzp/include/zp_cluster.h"

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

void CheckupMeta() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupMeta");
  std::vector<libzp::Node> slaves;
  libzp::Node master;
  slash::Status s = cluster->ListMeta(&master, &slaves);
  if (!s.ok()) {
    printf(RED "CheckupMeta, ListMeta Failed: %s" NONE "\n",
        s.ToString().c_str());
    health = false;
    return;
  }
  printf("Master: %s:%d\n", master.ip.c_str(), master.port);
  printf("Slaves: ");
  size_t i = 0;
  for (i = 0; i < slaves.size() - 1; i++) {
    printf("%s:%d, ", slaves[i].ip.c_str(), slaves[i].port);
  }
  if (i == slaves.size() - 1) {
    printf("%s:%d\n", slaves[i].ip.c_str(), slaves[i].port);
  } else {
    printf("\n");
  }
}

void CheckupNode() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupNode");
  std::vector<libzp::Node> nodes;
  std::vector<std::string> status;
  slash::Status s = cluster->ListNode(&nodes, &status);
  if (!s.ok()) {
    printf(RED "CheckupNode, ListNode Failed: %s" NONE "\n",
        s.ToString().c_str());
    health = false;
    return;
  }
  size_t alive_nodes_num = 0;
  for (size_t i = 0; i < status.size(); i++) {
    if (status[i] == "up") {
      alive_nodes_num++;
    }
  }
  printf("Nodes Count: %lu, Up: %lu, Down: ",
      nodes.size(), alive_nodes_num);
  if (alive_nodes_num == nodes.size()) {
    printf("0\n" GREEN " ...... PASSED\n" NONE);
  } else {
    health = false;
    printf(BROWN "%lu\n" NONE RED " ...... FAILED\n" NONE,
        nodes.size() - alive_nodes_num);
  }
}

void CheckupEpoch() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupEpoch");
  std::vector<std::string> tables;
  slash::Status s = cluster->ListTable(&tables);
  if (!s.ok()) {
    printf(RED "CheckupEpoch, ListTable Failed: %s" NONE "\n",
        s.ToString().c_str());
    health = false;
    return;
  }
  if (tables.empty()) {
    printf(RED "CheckupEpoch, No Table" NONE "\n");
    return;
  }
  s = cluster->Pull(tables[0]);
  if (!s.ok()) {
    printf(RED "CheckupEpoch, Pull Failed: %s" NONE "\n",
          s.ToString().c_str());
    return;
  }
  int meta_epoch = cluster->epoch();

  std::vector<libzp::Node> nodes;
  std::vector<std::string> status;
  s = cluster->ListNode(&nodes, &status);
  if (!s.ok()) {
    printf(RED "CheckupEpoch, ListNode Failed: %s" NONE "\n",
        s.ToString().c_str());
    return;
  }

  int dismatch_epoch_num = 0;
  for (size_t i = 0; i < nodes.size(); i++) {
    libzp::ServerState state;
    libzp::Status s = cluster->InfoServer(nodes[i], &state);
    if (!s.ok()) {
      printf(RED "CheckupEpoch, InfoServer Failed: %s" NONE "\n",
          s.ToString().c_str());
      dismatch_epoch_num++;
      continue;
    }
    if (state.epoch != meta_epoch) {
      dismatch_epoch_num++;
    }
  }

  printf("CurrentMetaEpoch: %d, DismatchNodesNum: ", meta_epoch);
  if (dismatch_epoch_num) {
    health = false;
    printf(BROWN "%d\n" NONE RED " ...... FAILED" NONE "\n",
        dismatch_epoch_num);
  } else {
    printf("0\n" GREEN "...... PASSED" NONE "\n");
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


void CheckupTable() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupTable");
  /*
   * Get the table list
   */
  std::vector<std::string> tables;
  slash::Status s = cluster->ListTable(&tables);
  if (!s.ok()) {
    printf(RED "CheckupTable, ListTable Failed: %s" NONE "\n",
        s.ToString().c_str());
    return;
  }
  if (tables.empty()) {
    printf(RED "CheckupTable, No Table" NONE "\n");
    return;
  }
  printf("Table Count: %lu\n", tables.size());

  // Pull every table to get its repl & offset info
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    s = cluster->Pull(*iter);
    if (!s.ok()) {
      printf(RED "CheckupTable, Pull Failed: %s" NONE "\n",
            s.ToString().c_str());
      return;
    }
  }

  /*
   * Check up every table
   */
  std::unordered_map<std::string, libzp::Table*> tables_info =
    cluster->tables();
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
    }

    if (dismatch_repl_num.empty() && lack_alive_repl_num.empty() &&
          dismatch_offset_num.empty()) {
      printf(GREEN "...... PASSED\n" NONE);
      continue;
    }

    health = false;
    printf(RED "...... FAILED\n" NONE);

    printf("--PartitionCount: %lu\n", partitions.size());
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
  }
}

void CheckupConclusion() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "CheckupConclusion");
  if (health) {
    printf(GREEN REVERSE "zeppelin HEALTHY\n" NONE);
  } else {
    printf(RED REVERSE "zeppelin UNHEALTHY\n" NONE);
  }
}

void Usage() {
  fprintf(stderr,
          "Usage: zp_chekup [host port]\n"
          "\thost     -- zeppelin meta ip(OPTIONAL default: 127.0.0.1) \n"
          "\tport     -- zeppelin meta port(OPTIONAL default: 9221) \n"
          "\t-h       -- show this help"
          "example: ./zp_checkup\n");
}

int main(int argc, char* argv[]) {
  if (argc > 3) {
    Usage();
    return 0;
  }

  std::string ip = "127.0.0.1";
  int port = 9221;
  if (argc > 1) {
    ip = argv[1];
    if (ip == "-h") {
      Usage();
      return 0;
    }
  }
  if (argc == 3) {
    port = std::atoi(argv[2]);
  }

  libzp::Options option;
  option.meta_addr.push_back(libzp::Node(ip, port));
  option.op_timeout = 5000;
  cluster = new libzp::Cluster(option);
  view_map.clear();
  CheckupMeta();
  CheckupNode();
  CheckupEpoch();
  CheckupTable();
  CheckupConclusion();

  return 0;
}
