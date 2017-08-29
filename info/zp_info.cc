#include <stdio.h>
#include <vector>
#include <unistd.h>

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
#define CLRLINE              "\r\e[K" //or "\e[1K\r"

libzp::Cluster* cluster;
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
  printf(L_PURPLE "Nodes Count:\n" NONE);
  printf("  %lu\n", nodes.size());
  printf(L_PURPLE "Detail:\n" NONE);
  for (size_t i = 0; i < nodes.size();) {
    printf(REVERSE "%15s:%5d" NONE" ", nodes[i].ip.c_str(), nodes[i].port);
    if (i + 1 < nodes.size()) {
      printf(REVERSE "%15s:%5d" NONE" ", nodes[i + 1].ip.c_str(), nodes[i + 1].port);
    }
    if (i + 2 < nodes.size()) {
      printf(REVERSE "%15s:%5d" NONE" ", nodes[i + 2].ip.c_str(), nodes[i + 2].port);
    }

    printf("\n");

    int num = 0;
    while (num < 3 && i < nodes.size()) {
      if (status[i] == "up") {
        printf(GREEN "%21s " NONE, "UP"); 
      } else {
        printf(RED "%21s " NONE, "DOWN"); 
      }
      i++;
      num++;
    }
    printf("\n");
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
      printf("%10ld %15s:%5d ",state.epoch, state.cur_meta.ip.c_str(),
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


void InfoMeta() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoMeta");
  std::vector<libzp::Node> slaves;
  libzp::Node master;
  slash::Status s = cluster->ListMeta(&master, &slaves);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    return;
  }
  printf(L_PURPLE "Leader:\n" NONE);
  printf("  %s:%d\n", master.ip.c_str(), master.port);
  printf(L_PURPLE "Followers:\n" NONE);
  for (auto iter = slaves.begin(); iter != slaves.end(); iter++) {
    printf("  %s:%d\n", iter->ip.c_str(), iter->port);
  }
  std::string meta_status;
  printf(L_PURPLE "Detail:\n" NONE);
  s = cluster->MetaStatus(&meta_status);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
  }
  printf("%s", meta_status.c_str());
}

void PartitionDetail(const std::string& table, int32_t id,
    const std::string& ip, int32_t port) {
  std::map<int, libzp::PartitionView> view;
  libzp::Node node(ip, port);
  slash::Status s = cluster->InfoRepl(node, table, &view);
  if (!s.ok()) {
    printf("%15s:%5d" RED "%49s" NONE, ip.c_str(), port,"[Maybe DOWN]");
    return;
  }
  auto iter = view.find(id);
  if (iter == view.end()) {
//    printf(RED "Failed: %s-%d Not Found" NONE "\n", table.c_str(), id);
    return;
  }
  printf("%15s:%5d[%s, %s, %10d %11ld] ", ip.c_str(), port,
      iter->second.role.c_str(), iter->second.repl_state.c_str(),
      iter->second.offset.filenum, iter->second.offset.offset);
}

void InfoTable(bool detail = false) {
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
  printf("\n");
  printf(L_PURPLE "Detail:\n" NONE);
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    s = cluster->Pull(*iter);
    std::unordered_map<std::string, libzp::Table*> tables = cluster->tables();
    auto it = tables.find(*iter);
    if (it == tables.end()) {
      printf(RED "Failed: %s Not Found" NONE "\n", (*iter).c_str());
      continue;
    }
    printf("\e[0;35m%s [%d]\e[0m\n", it->second->table_name().c_str(),
        it->second->partition_num());
    printf("\e[7m%5s\e[0m \e[7m%10s\e[0m \e[7m%70s\e[0m \e[7m%140s\e[0m\n",
        "Id", "Active", "Master", "Slaves");
    std::map<int, libzp::Partition> partitions = it->second->partitions();
    for (auto& par : partitions) {
      printf("%5d ", par.second.id());
      if (par.second.active()) {
        printf("\e[0;32m%10s\e[0m ", "Yes");
      } else {
        printf("\e[0;31m%10s\e[0m ", "No");
      }
      if (detail) {
        PartitionDetail(*iter, par.second.id(),
            par.second.master().ip,
            par.second.master().port);
        printf(" ");
        for (auto& s : par.second.slaves()) {
          PartitionDetail(*iter, par.second.id(), s.ip,
              s.port);
        }
      } else {
        printf("%64s:%5d ", par.second.master().ip.c_str(),
            par.second.master().port);
        for (auto& s : par.second.slaves()) {
          printf("%15s:%5d ", s.ip.c_str(), s.port);
        }
      }
      printf("\n");
    }
    std::map<libzp::Node, std::vector<const libzp::Partition*>> nodes_loads;
    it->second->GetNodesLoads(&nodes_loads);
    for (auto& node : nodes_loads) {
      std::cout << node.first << ": [";
      const std::vector<const libzp::Partition*>& p_vec = node.second;
      const libzp::Partition* p;
      size_t i = 0;
      for (i = 0; i < p_vec.size() - 1; i++) {
        p = p_vec.at(i);
        if (p->master() == node.first) {
          printf("\e[0;35m%d*,\e[0m", p->id());
        } else {
          printf("%d,", p->id());
        }
      }
      p = p_vec.at(i);
      if (p->master() == node.first) {
        printf("\e[0;35m%d*\e[0m]\n", p->id());
      } else {
        printf("%d]\n", p->id());
      }
    }
  }
}

void InfoQuery() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoQuery");
  std::vector<std::string> tables;
  slash::Status s = cluster->ListTable(&tables);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
  }
  printf(REVERSE "%35s" NONE" " REVERSE "%10s" NONE" " REVERSE "%15s" NONE"\n",
      "Table", "QPS", "TotalQuery");
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    int32_t qps = 0; int64_t total_query = 0;
    s = cluster->InfoQps(*iter, &qps, &total_query);
    if (!s.ok()) {
      printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    }
    printf("%35s %10d %15ld\n", (*iter).c_str(), qps, total_query);
  }
}

void InfoSpace() {
  printf(BLUE UNDERLINE "%-140s\n" NONE, "InfoSpace");
  std::vector<std::string> tables;
  slash::Status s = cluster->ListTable(&tables);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
  }
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    printf(PURPLE "%s" NONE"\n", (*iter).c_str());
    std::vector<std::pair<libzp::Node, libzp::SpaceInfo>> nodes;
    libzp::Status s = cluster->InfoSpace(*iter, &nodes);
    if (!s.ok()) {
      std::cout << "Failed: " << s.ToString() << std::endl;
    }
    printf(REVERSE "%21s" NONE" " REVERSE "%15s" NONE" " REVERSE "%15s" NONE"\n",
      "Node", "Used", "Remain");
    for (auto it = nodes.begin(); it != nodes.end(); it++) {
      printf("%15s:%5d %15ld %15ld\n", it->first.ip.c_str(), it->first.port,
          it->second.used, it->second.remain);
    }
  }
}

void Usage() {
  fprintf(stderr,
          "Usage: zp_info [-h host -p port -i info]\n"
          "\t-h     -- zeppelin meta ip(OPTIONAL default: 127.0.0.1) \n"
          "\t-p     -- zeppelin meta port(OPTIONAL default: 9221) \n"
          "\t-i     -- info field [node, nodedetail, meta, table, tabledetail, query, space, all, alldetail](REQUIRE)\n"
          "  example: ./zp_info -i all\n"
         );
}

int main(int argc, char* argv[]) {
  if (argc == 1) {
    Usage();
    return 0;
  }
  libzp::Options option;
  std::string ip = "127.0.0.1";
  int port = 9221;
  std::string info = "";
  char buf[1024];
  char c;
  while (-1 != (c = getopt(argc, argv, "h:p:i:"))) {
    switch (c) {
      case 'h':
        snprintf(buf, 1024, "%s", optarg);
        ip = std::string(buf);
        break;
      case 'p':
        snprintf(buf, 1024, "%s", optarg);
        port = std::atoi(buf);
        break;
      case 'i':
        snprintf(buf, 1024, "%s", optarg);
        info = buf;
        break;
      default:
        Usage();
        return 0;
    }
  }
  if (info == "") {
    Usage();
    return 0;
  }
  option.meta_addr.push_back(libzp::Node(ip, port));
  option.op_timeout = 5000;
  // cluster handle cluster operation
  cluster = new libzp::Cluster(option);
  if (info == "node") {
    InfoNode();
  } else if (info == "nodedetail") {
    InfoNodeDetail();
  } else if (info == "meta") {
    InfoMeta();
  } else if (info == "table") {
    InfoTable(false);
  } else if (info == "tabledetail") {
    InfoTable(true);
  } else if (info == "query") {
    InfoQuery();
  } else if (info == "space") {
    InfoSpace();
  } else if (info == "all") {
    InfoNode();
    printf("\n");
    InfoMeta();
    printf("\n");
    InfoTable();
    printf("\n");
    InfoQuery();
    printf("\n");
    InfoSpace();
  } else if (info == "alldetail") {
    InfoNodeDetail();
    printf("\n");
    InfoMeta();
    printf("\n");
    InfoTable(true);
    printf("\n");
    InfoQuery();
    printf("\n");
    InfoSpace();
  } else {
    Usage();
  }

  return 0;
}
