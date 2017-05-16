/*
 * "Copyright [2016] qihoo"
 */
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "libzp/include/zp_cluster.h"
#include "linenoise/linenoise.h"
#include "zp_manager_help.h"

void SplitByBlank(const std::string& old_line,
    std::vector<std::string>& line_args) {
  std::string line = old_line;
  line += " ";
  std::string unparse = line;
  std::string::size_type pos_start;
  std::string::size_type pos_end;
  pos_start = unparse.find_first_not_of(" ");
  while (pos_start != std::string::npos) {
    pos_end = unparse.find_first_of(" ", pos_start);
    line_args.push_back(unparse.substr(pos_start, pos_end - pos_start));
    unparse = unparse.substr(pos_end);
    pos_start = unparse.find_first_not_of(" ");
  }
}

typedef struct {
  std::string name;
  std::string params;
  int params_num;
  std::string info;
} CommandEntry;


static std::vector<CommandEntry> commandEntries;
static int helpEntriesLen;

static void cliInitHelp(void) {
  int commands_num = sizeof(commandHelp)/sizeof(struct CommandHelp);
  int i, len, pos = 0;
  CommandEntry tmp;
  for (int i = 0; i < commands_num; i++) {
    tmp.name = std::string(commandHelp[i].name);
    tmp.params = std::string(commandHelp[i].params);
    tmp.params_num = commandHelp[i].params_num;
    tmp.info = std::string(commandHelp[i].summary);
    commandEntries.push_back(tmp);
  }
}

// completion example
void completion(const char *buf, linenoiseCompletions *lc) {
  size_t start_pos = 0;
  size_t match_len = 0;
  std::string tmp;

  for (size_t i = 0; i < commandEntries.size(); i++) {
    match_len = strlen(buf);
    if (strncasecmp(buf, commandEntries[i].name.c_str(), match_len) == 0) {
      tmp = std::string();
      tmp = commandEntries[i].name;
      linenoiseAddCompletion(lc, tmp.c_str());
    }
  }
}

// hints example
char *hints(const char *buf, int *color, int *bold) {
  std::string buf_str = std::string(buf);
  std::vector<std::string> buf_args;
  SplitByBlank(buf_str, buf_args);
  size_t buf_len = strlen(buf);
  if (buf_len == 0) {
    return NULL;
  }
  int endspace = buf_len && isspace(buf[buf_len-1]);
  for (size_t i = 0; i < commandEntries.size(); i++) {
    size_t match_len = std::max(strlen(commandEntries[i].name.c_str()),
        strlen(buf_args[0].c_str()));
    if (strncasecmp(buf_args[0].c_str(),
          commandEntries[i].name.c_str(), match_len) == 0) {
      *color = 90;
      *bold = 0;
      char* hint = const_cast<char *>(commandEntries[i].params.c_str());
      int to_move = buf_args.size() - 1;
      while (strlen(hint) && to_move > 0) {
        if (hint[0] == ' ') {
          to_move--;
        }
        hint = hint + 1;
      }
      if (!endspace) {
        std::string new_hint = std::string(" ") + std::string(hint);
        hint = const_cast<char *>(new_hint.c_str());
      }
      return strlen(hint)? hint:NULL;
    }
  }
  return NULL;
}


void StartRepl(libzp::Cluster* cluster) {
  char *line;
  linenoiseSetMultiLine(1);
  linenoiseSetCompletionCallback(completion);
  linenoiseSetHintsCallback(hints);
  linenoiseHistoryLoad("history.txt"); /* Load the history at startup */

  libzp::Status s;
  while ((line = linenoise("zp >> ")) != NULL) {
    linenoiseHistoryAdd(line); /* Add to the history. */
    linenoiseHistorySave("history.txt"); /* Save the history on disk. */
    /* Do something with the string. */
    std::string info = line;
    std::vector<std::string> line_args;
    SplitByBlank(info, line_args);

    if (!strncasecmp(line, "CREATE ", 7)) {
      std::string table_name = line_args[1];
      int partition_num = atoi(line_args[2].c_str());
      s = cluster->CreateTable(table_name, partition_num);
      std::cout << s.ToString() << std::endl;
      std::cout << "repull table "<< table_name << std::endl;
      s = cluster->Pull(table_name);

    } else if (!strncasecmp(line, "PULL ", 5)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      s = cluster->Pull(table_name);
      std::cout << s.ToString() << std::endl;
      std::cout << "current table info:" << std::endl;
      cluster->DebugDumpPartition(table_name);

    } else if (!strncasecmp(line, "DUMP ", 5)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      cluster->DebugDumpPartition(table_name);

    } else if (!strncasecmp(line, "LOCATE ", 5)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      int partition_id = cluster->LocateKey(line_args[1], line_args[2]);
      if (partition_id >= 0) {
        cluster->DebugDumpPartition(line_args[1], partition_id);
      } else {
        std::cout << "doe not exist in local table" << std::endl;
      }

    } else if (!strncasecmp(line, "SET ", 4)) {
      if (line_args.size() != 4
          && line_args.size() != 5) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::string key = line_args[2];
      std::string value = line_args[3];
      int ttl = -1;
      if (line_args.size() == 5) {
        int ttl = std::strtol(line_args[4].c_str(), NULL, 10);
        if (*end != 0) {
          std::cout << "ttl must be a integer" << std::endl;
          continue;
        }
      }
      s = cluster->Set(table_name, key, value, ttl);
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "SETMASTER ", 10)) {
      if (line_args.size() != 5) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition = atoi(line_args[2].c_str());
      libzp::Node node(line_args[3], atoi(line_args[4].c_str()));
      s = cluster->SetMaster(table_name, partition, node);
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "ADDSLAVE ", 9)) {
      if (line_args.size() != 5) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition = atoi(line_args[2].c_str());
      libzp::Node node(line_args[3], atoi(line_args[4].c_str()));
      s = cluster->AddSlave(table_name, partition, node);
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "REMOVESLAVE ", 12)) {
      if (line_args.size() != 5) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition = atoi(line_args[2].c_str());
      libzp::Node node(line_args[3], atoi(line_args[4].c_str()));
      s = cluster->RemoveSlave(table_name, partition, node);
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "GET ", 4)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::string key = line_args[2];
      std::string value;
      s = cluster->Get(table_name, key, &value);
      if (s.ok()) {
        std::cout << value << std::endl;
      } else {
        std::cout << s.ToString() << std::endl;
      }
    } else if (!strncasecmp(line, "LISTMETA", 8)) {
        if (line_args.size() != 1) {
          std::cout << "arg num wrong" << std::endl;
          continue;
        }
        std::vector<libzp::Node> slaves;
        libzp::Node master;
        s = cluster->ListMeta(&master, &slaves);
        std::cout << "master" << ":" << master.ip
          << " " << master.port << std::endl;
        std::cout << "slave" << ":" << std::endl;
        std::vector<libzp::Node>::iterator iter = slaves.begin();
        while (iter != slaves.end()) {
          std::cout << iter->ip << ":" << iter->port << std::endl;
          iter++;
        }
        std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "LISTNODE", 8)) {
        if (line_args.size() != 1) {
          std::cout << "arg num wrong" << std::endl;
          continue;
        }
        std::vector<libzp::Node> nodes;
        std::vector<std::string> status;
        s = cluster->ListNode(&nodes, &status);
        if (nodes.size() == 0) {
          std::cout << "no node exist" << std::endl;
          continue;
        }
        for (size_t i = 0; i <= nodes.size() - 1; i++) {
          std::cout << nodes[i].ip << ":" << nodes[i].port
            << " " << status[i] << std::endl;
        }

    } else if (!strncasecmp(line, "LISTTABLE", 9)) {
        if (line_args.size() != 1) {
          std::cout << "arg num wrong" << std::endl;
          continue;
        }
        std::vector<std::string> tables;
        s = cluster->ListTable(&tables);
        std::vector<std::string>::iterator iter = tables.begin();
        while (iter != tables.end()) {
          std::cout << *iter << std::endl;
          iter++;
        }
        std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "DROPTABLE ", 10)) {
        if (line_args.size() != 2) {
          std::cout << "arg num wrong" << std::endl;
          continue;
        }
        s = cluster->DropTable(line_args[1]);
        std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "QPS", 3)) {
        if (line_args.size() != 2) {
          std::cout << "arg num wrong" << std::endl;
          continue;
        }
        std::string table_name = line_args[1];
        int qps = 0, total_query = 0;
        s = cluster->InfoQps(table_name, &qps, &total_query);
        std::cout << "qps:" << qps << std::endl;
        std::cout << "total query:" << total_query << std::endl;

    } else if (!strncasecmp(line, "REPLSTATE", 9)) {
        if (line_args.size() != 4) {
          std::cout << "arg num wrong" << std::endl;
          continue;
        }
        std::string table_name = line_args[1];
        std::string ip = line_args[2];
        int port = atoi(line_args[3].c_str());
        libzp::Node node(ip, port);
        std::map<int, libzp::PartitionView> partitions;
        libzp::Status s = cluster->InfoRepl(node, table_name, &partitions);
        for (auto& p : partitions) {
          std::cout << "partition:" << p.first << std::endl;
          std::cout << " -role:" << p.second.role << std::endl;
          std::cout << " -repl_state:" << p.second.repl_state << std::endl;
          std::cout << " -master:" << p.second.master.ip <<
            ":" << p.second.master.port << std::endl;
          std::cout << " -slaves:" << std::endl;
          for (auto& s : p.second.slaves) {
            std::cout << "  -slave:" << s.ip << ":" << s.port << std::endl;
          }
          std::cout << " -filenum:" << p.second.file_num << std::endl;
          std::cout << " -offset:" << p.second.offset << std::endl;
        }

    } else if (!strncasecmp(line, "SPACE", 5)) {
       if (line_args.size() != 2) {
         std::cout << "arg num wrong" << std::endl;
         continue;
       }
       std::string table_name = line_args[1];
       std::vector<std::pair<libzp::Node, libzp::SpaceInfo>> nodes;
       libzp::Status s = cluster->InfoSpace(table_name, &nodes);
       std::cout << "space info for " << table_name << std::endl;
       for (size_t i = 0; i < nodes.size(); i++) {
         std::cout << "node: " << nodes[i].first.ip << " " <<
           nodes[i].first.port << std::endl;
         std::cout << "  used:" << nodes[i].second.used
           << " bytes" << std::endl;
         std::cout << "  remain:" << nodes[i].second.remain
           << " bytes" << std::endl;
       }
    } else if (!strncasecmp(line, "NODESTATE", 9)) {
       if (line_args.size() != 3) {
         std::cout << "arg num wrong" << std::endl;
         continue;
       }
       std::string ip = line_args[1];
       int port = atoi(line_args[2].c_str());
       libzp::Node node(ip, port);
       libzp::ServerState state;
       libzp::Status s = cluster->InfoServer(node, &state);
       std::cout << "node: (" << node.ip << ":" << node.port << ")" << std::endl;
       std::cout << " -epoch:" << state.epoch << std::endl;
       std::cout << " -tables:" << std::endl;
       for (auto& t : state.table_names) {
         std::cout << "  -table:" << t << std::endl;
       }
       std::cout << " -current_meta: " << state.cur_meta.ip
         << ":" << state.cur_meta.port << std::endl;
       std::cout << " -meta_renewing:"
         << (state.meta_renewing ? "true" : "false") << std::endl;
    } else if (!strncasecmp(line, "EXIT", 4)) {
       // Exit manager
       free(line);
       break;
    } else {
      printf("Unrecognized command: %s\n", line);
    }
    free(line);
  }
  std::cout << "out of loop" << std::endl;
}

void usage() {
  std::cout << "usage:\n"
            << "      zp_cli host port\n";
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    usage();
    return -1;
  }
  std::cout << "start" << std::endl;
  libzp::Options option;
  libzp::Node node(argv[1], atoi(argv[2]));
  option.meta_addr.push_back(node);

  // cluster handle cluster operation
  std::cout << "create cluster" << std::endl;
  libzp::Cluster* cluster = new libzp::Cluster(option);
  std::cout << "connect cluster" << std::endl;
  // needs connect to cluster first
  libzp::Status s = cluster->Connect();
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    exit(-1);
  }

  cliInitHelp();
  StartRepl(cluster);
  /*
  Status s = cluster.ListMetaNode(node_list);
  node_list.clear();
  s = cluster.ListDataNode(node_list);
  */
}
