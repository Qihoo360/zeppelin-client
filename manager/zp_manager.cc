/*
 * "Copyright [2016] qihoo"
 */
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "slash/include/slash_string.h"
#include "libzp/include/zp_cluster.h"
#include "linenoise.h"
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

std::string TimeString(uint64_t nowmicros) {
  time_t t = static_cast<time_t>(nowmicros * 1e-6);
  char buf[100];
  struct tm t_ = *gmtime(&t);
  strftime(buf, sizeof buf, "%a, %d %b %Y %H:%M:%S %Z", &t_);
  return std::string(buf);
}

typedef struct {
  std::string name;
  std::string params;
  int params_num;
  std::string info;
} CommandEntry;


static std::vector<CommandEntry> commandEntries;

static void cliInitHelp(void) {
  int commands_num = sizeof(commandHelp)/sizeof(struct CommandHelp);
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
  for (size_t i = 0; i < commandEntries.size(); i++) {
    size_t match_len = strlen(buf);
    if (strncasecmp(buf, commandEntries[i].name.c_str(), match_len) == 0) {
      std::string tmp = commandEntries[i].name;
      linenoiseAddCompletion(lc, tmp.c_str());
    }
  }
}

// hints callback
char hint_buf[100] = {0};
char *hints_callback(const char *buf, int *color, int *bold) {
  std::string buf_str = std::string(buf);
  std::vector<std::string> buf_args;
  SplitByBlank(buf_str, buf_args);
  size_t buf_len = strlen(buf);
  if (buf_len == 0) {
    return nullptr;
  }
  int endspace = buf_len && isspace(buf[buf_len-1]);
  char* hint = nullptr;
  for (size_t i = 0; i < commandEntries.size(); i++) {
    size_t match_len = std::max(strlen(commandEntries[i].name.c_str()),
        strlen(buf_args[0].c_str()));
    if (strncasecmp(buf_args[0].c_str(),
          commandEntries[i].name.c_str(), match_len) == 0) {
      *color = 90;
      *bold = 0;
      hint = hint_buf;
      snprintf(hint, 100, "%s", commandEntries[i].params.c_str());
      int to_move = buf_args.size() - 1;
      while (strlen(hint) && to_move > 0) {
        if (hint[0] == '[') break;
        if (hint[0] == ' ') {
          to_move--;
        }
        hint = hint + 1;
      }
      if (!endspace) {
        char buf[100];
        snprintf(buf, 100, " %s", hint);
        strncpy(hint_buf, buf, 100);
        hint = hint_buf;
      }
      return strlen(hint) ? hint : nullptr;
    }
  }
  return hint;
}

const int64_t KB = 1024;
const int64_t MB = KB << 10;
const int64_t GB = MB << 10;
const int64_t TB = GB << 10;
const int64_t PB = TB << 10;

static std::string to_human(int64_t bytes) {
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

const char* history_file = "/tmp/zp_manager_history.txt";

void StartRepl(libzp::Cluster* cluster) {
  char *line;
  linenoiseSetMultiLine(1);
  linenoiseSetCompletionCallback(completion);
  linenoiseSetHintsCallback(hints_callback);
  linenoiseHistoryLoad(history_file); /* Load the history at startup */

  libzp::Status s;
  while ((line = linenoise("zp >> ")) != nullptr) {
    linenoiseHistoryAdd(line); /* Add to the history. */
    linenoiseHistorySave(history_file); /* Save the history on disk. */
    /* Do something with the string. */
    std::string info = line;
    std::vector<std::string> line_args;
    SplitByBlank(info, line_args);
    
    if (!strncasecmp(line, "CREATE ", 7)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
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
        char* end = nullptr;
        ttl = std::strtol(line_args[4].c_str(), &end, 10);
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
    } else if (!strncasecmp(line, "EXPAND ", 7)) {
      if (line_args.size() < 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::vector<libzp::Node> new_nodes;
      for (size_t i = 2; i< line_args.size(); ++i) {
        std::string ip;
        int port = -1;
        if (!slash::ParseIpPortString(line_args[i], ip, port)) {
          printf("unknow ip:port format, %s\n", line_args[i].c_str());
          continue;
        }
        new_nodes.push_back(libzp::Node(ip, port));
      }
      printf("Adding new nodes to [%s]:\n", table_name.c_str());
      for (auto& node : new_nodes) {
        printf("   --- %s:%d\n", node.ip.c_str(), node.port);
      }

      s = cluster->Expand(table_name, new_nodes);
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "SHRINK ", 7)) {
      if (line_args.size() < 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::vector<libzp::Node> deleting;
      for (size_t i = 2; i< line_args.size(); ++i) {
        std::string ip;
        int port = -1;
        if (!slash::ParseIpPortString(line_args[i], ip, port)) {
          printf("unknow ip:port format, %s\n", line_args[i].c_str());
          continue;
        }
        deleting.push_back(libzp::Node(ip, port));
      }
      printf("Deleting nodes of [%s]:\n", table_name.c_str());
      for (auto& node : deleting) {
        printf("   --- %s:%d\n", node.ip.c_str(), node.port);
      }

      s = cluster->Shrink(table_name, deleting);
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
    } else if ((!strncasecmp(line, "MGET ", 5))) {
      if (line_args.size() < 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::vector<std::string> keys;
      std::map<std::string, std::string> values;
      for (size_t i = 2; i < line_args.size(); ++i) {
        keys.push_back(line_args[i]);
      }

      s = cluster->Mget(table_name, keys, &values);
      if (s.ok()) {
        std::map<std::string, std::string>::iterator it = values.begin();
        for (; it != values.end(); ++it) {
          std::cout << it->first << "=>" << it->second << std::endl;
        }
      } else {
        std::cout << s.ToString() << std::endl;
      }
    } else if (!strncasecmp(line, "DELETE ", 6)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::string key = line_args[2];
      s = cluster->Delete(table_name, key);
      if (s.ok()) {
        std::cout << "delete " << table_name << " " << key << " success" << std::endl;
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
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
        continue;
      }
      std::cout << "master" << ":" << master.ip
        << " " << master.port << std::endl;
      std::cout << "slave" << ":" << std::endl;
      std::vector<libzp::Node>::iterator iter = slaves.begin();
      while (iter != slaves.end()) {
        std::cout << iter->ip << ":" << iter->port << std::endl;
        iter++;
      }
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "METASTATUS", 10)) {

      if (line_args.size() != 1) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string meta_status;
      s = cluster->MetaStatus(&meta_status);
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
        continue;
      }
      std::cout << std::string(140, '=') << std::endl;
      std::cout << meta_status << std::endl;
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "LISTNODE", 8)) {
      if (line_args.size() != 1) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::vector<libzp::Node> nodes;
      std::vector<std::string> status;
      s = cluster->ListNode(&nodes, &status);
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
        continue;
      }
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
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
        continue;
      }
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

    } else if (!strncasecmp(line, "FLUSHTABLE ", 11)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      s = cluster->FlushTable(line_args[1]);
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "QPS", 3)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int32_t qps = 0; int64_t total_query = 0;
      s = cluster->InfoQps(table_name, &qps, &total_query);
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
        continue;
      }
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
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
      }
      for (auto& p : partitions) {
        std::cout << "partition:" << p.first << std::endl;
        std::cout << " -role:" << p.second.role << std::endl;
        std::cout << " -repl_state:" << p.second.repl_state << std::endl;
        std::cout << " -master:" << p.second.master.ip <<
          ":" << p.second.master.port << std::endl;
        std::cout << " -slaves:" << std::endl;
        for (auto& pss : p.second.slaves) {
          std::cout << "  -slave:" << pss.ip << ":" << pss.port << std::endl;
          continue;
        }
        std::cout << " -boffset:" << p.second.offset << std::endl;
        if (p.second.fallback_time != 0) {
          std::cout << " -Notice! has binlog fallback" << std::endl;
          std::cout << "  -time:"
            << TimeString(p.second.fallback_time) << std::endl;
          std::cout << "  -before: " << p.second.fallback_before << std::endl;  
          std::cout << "  -after: " << p.second.fallback_after << std::endl;  
        }
      }

    } else if (!strncasecmp(line, "SPACE", 5)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::vector<std::pair<libzp::Node, libzp::SpaceInfo>> nodes;
      libzp::Status s = cluster->InfoSpace(table_name, &nodes);
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
        continue;
      }
      std::cout << "space info for " << table_name << std::endl;
      for (size_t i = 0; i < nodes.size(); i++) {
        std::cout << "node: " << nodes[i].first.ip << " " <<
          nodes[i].first.port << std::endl;
        int64_t used = nodes[i].second.used;
        int64_t remain = nodes[i].second.remain;
        printf("  used: %ld bytes(%s)\n", used, to_human(used).c_str());
        printf("  remain: %ld bytes(%s)\n", remain, to_human(remain).c_str());
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
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
        continue;
      }
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

  cliInitHelp();
  StartRepl(cluster);
}
