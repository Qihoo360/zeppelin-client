/*
 * "Copyright [2016] qihoo"
 */
#include <cstring>
#include <cstdio>
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "slash/include/slash_string.h"
#include "libzp/include/zp_cluster.h"
#include "utils/linenoise.h"
#include "utils/distribution.h"

struct CommandHelp {
  std::string name;
  std::string params;
  int params_num;
  std::string summary;
};

CommandHelp commandHelp[] = {
  { "SET",
    "table key value [ttl]",
    3,
    "Set key"},

  { "GET",
    "table key",
    2,
    "Get key's value"},

  { "MGET",
    "table key [key ...]",
    2,
    "Get the values of all the given keys"},

  { "DELETE",
    "table key",
    2,
    "Delete key"},

  { "CREATETABLE",
    "table host_list_file",
    2,
    "Create table, 3 partition per node as default"},

  { "ADDMETANODE",
    "ip:addr",
    1,
    "Add new meta node to cluster"},

  { "REMOVEMETANODE",
    "ip:addr",
    1,
    "Remove existing meta node from cluster"},

  { "PULL",
    "table",
    1,
    "Pull table info"},

  { "SETMASTER",
    "table partition ip:port",
    3,
    "Set a partition's master"},

  { "ADDSLAVE",
    "table partition ip:port",
    3,
    "Add master for partition"},

  { "REMOVESLAVE",
    "table partition ip:port",
    3,
    "Remove master for partition"},

  { "REMOVENODES",
    "ip:port [ip:port ...]",
    1,
    "Remove nodes from cluster"},

  { "EXPAND",
    "table ip:port [ip:port ...]",
    2,
    "Expand specific table's capacity"},

  { "EXPANDALLTABLE",
    "ip:port [ip:port ...]",
    1,
    "Expand all tables' capacity"},

  { "MIGRATE",
    "table source(ip:port) partition_id destination(ip:port)",
    4,
    "Change table's distribution"},

  { "SHRINK",
    "table ip:port [ip:port ...]",
    2,
    "Shrink specific table's capacity"},

  { "SHRINKALLTABLE",
    "ip:port [ip:port ...]",
    1,
    "Shrink all tables' capacity"},

  { "REPLACENODE",
    "source(ip:port) dstination(ip:port)",
    2,
    "Move all loads from source to destination"},

  { "CANCELMIGRATE",
    "",
    0,
    "Cancel zeppelin migrate"},

  { "LISTTABLE",
    "",
    0,
    "List all tables"},

  { "DROPTABLE",
    "table",
    1,
    "Drop one table"},

  { "FLUSHTABLE",
    "table",
    1,
    "Clean one table"},

  { "LISTNODE",
    "",
    0,
    "List all data nodes"},

  { "LISTMETA",
    "",
    0,
    "List all meta nodes"},

  { "METASTATUS",
    "",
    0,
    "List meta internal details"},

  { "QPS",
    "table",
    1,
    "Get qps for a table"},

  { "LATENCY",
    "table",
    1,
    "Get latency infomation for a table"},

  { "REPLSTATE",
    "table ip:port",
    2,
    "Check replication state"},

  { "NODESTATE",
    "ip:port",
    1,
    "Check node server state"},

  { "SPACE",
    "table",
    1,
    "Get space info for a table"},

  { "DUMP",
    "table",
    1,
    "Get space info for a table"},

  { "LOCATE",
    "table key",
    2,
    "Locate a key, find corresponding nodes"},

  { "HELP",
    "",
    0,
    "List all commands"},

  { "EXIT",
    "",
    0,
    "Exit the zp_manager"}
};

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
  if (buf_len == 0 || buf_args.empty()) {
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

void StartRepl(libzp::Cluster* cluster, const char* ip, int port) {
  char *line;
  linenoiseSetMultiLine(1);
  linenoiseSetCompletionCallback(completion);
  linenoiseSetHintsCallback(hints_callback);
  linenoiseHistoryLoad(history_file); /* Load the history at startup */

  libzp::Status s;
  char prompt[100];
  snprintf(prompt, 100, "Zeppelin(%s:%d)>> ", ip, port);
  while ((line = linenoise(prompt)) != nullptr) {
    linenoiseHistoryAdd(line); /* Add to the history. */
    linenoiseHistorySave(history_file); /* Save the history on disk. */
    /* Do something with the string. */
    std::string info = line;
    std::vector<std::string> line_args;
    SplitByBlank(info, line_args);

    if (!strncasecmp(line, "CREATETABLE ", 12)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      if (!distribution::Load(line_args[2])) {
        std::cout << "Cannot load file " << line_args[1] << std::endl;
        continue;
      }
      const int kDistributionTimes = 3;
      for (int times = 0; times < kDistributionTimes; times++) {
        distribution::Distribution();
      }
      distribution::Checkup();

      std::vector<std::vector<libzp::Node>> distribution;
      for (auto& par : distribution::result) {
        std::vector<libzp::Node> libzp_par;
        for (auto& node : par) {
          libzp_par.push_back(libzp::Node(node.host));
        }
        distribution.push_back(libzp_par);
      }
      distribution::Cleanup();

      char buf[16];
      while (true) {
        printf("Continue? (Y/N/dump)\n");
        fgets(buf, sizeof(buf), stdin);
        if (std::strcmp(buf, "dump\n") == 0) {
          // Dump
          printf("Table name: %s\n", table_name.c_str());
          size_t master_index = 0;
          for (size_t i = 0; i < distribution.size(); i++) {
            const std::vector<libzp::Node>& partition = distribution[i];
            master_index = (master_index + 1) % partition.size();
            printf("Partition id: %lu ", i);

            const libzp::Node& master_node = partition[master_index];
            printf("master: %s:%d ", master_node.ip.c_str(), master_node.port);

            int slave_index = 0;
            for (size_t j = 0; j < partition.size(); j++) {
              if (j == master_index) {
                continue;
              }
              const libzp::Node& node = partition[j];
              printf("slave%d: %s:%d ", slave_index++, node.ip.c_str(), node.port);
            }
            printf("\n");
          }
        } else if (std::tolower(buf[0]) == 'y' ||
                   std::tolower(buf[0]) == 'n') {
          break;
        }
      }
      if (std::tolower(buf[0]) == 'n') {
        continue;
      }

      s = cluster->CreateTable(table_name, distribution);
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "ADDMETANODE ", 12)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string ip;
      int port;
      slash::ParseIpPortString(line_args[1], ip, port);
      libzp::Node node_to_add(ip, port);
      s = cluster->RemoveMetaNode(node_to_add);
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "REMOVEMETANODE ", 15)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string ip;
      int port;
      slash::ParseIpPortString(line_args[1], ip, port);
      libzp::Node node_to_remove(ip, port);
      s = cluster->RemoveMetaNode(node_to_remove);
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "PULL ", 5)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      s = cluster->Pull(table_name);
      if (s.ok()) {
        std::cout << "current table info:" << std::endl;
        cluster->DebugDumpPartition(table_name);
      }
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "DUMP ", 5)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      cluster->DebugDumpPartition(
        table_name,
        -1 /* Dump all partitions */,
        true /* Dump nodes load info */);

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
      if (line_args.size() != 4) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition = atoi(line_args[2].c_str());
      std::string ip;
      int port;
      slash::ParseIpPortString(line_args[3], ip, port);
      libzp::Node node(ip, port);
      s = cluster->SetMaster(table_name, partition, node);
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "ADDSLAVE ", 9)) {
      if (line_args.size() != 4) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition = atoi(line_args[2].c_str());
      std::string ip;
      int port;
      slash::ParseIpPortString(line_args[3], ip, port);
      libzp::Node node(ip, port);
      s = cluster->AddSlave(table_name, partition, node);
      std::cout << s.ToString() << std::endl;

    } else if (!strncasecmp(line, "REMOVESLAVE ", 12)) {
      if (line_args.size() != 4) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition = atoi(line_args[2].c_str());
      std::string ip;
      int port;
      slash::ParseIpPortString(line_args[3], ip, port);
      libzp::Node node(ip, port);
      s = cluster->RemoveSlave(table_name, partition, node);
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "REMOVENODES ", 12)) {
      if (line_args.size() < 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }

      std::vector<libzp::Node> nodes;
      for (size_t i = 1; i< line_args.size(); ++i) {
        std::string ip;
        int port = -1;
        if (!slash::ParseIpPortString(line_args[i], ip, port)) {
          printf("unknow ip:port format, %s\n", line_args[i].c_str());
          continue;
        }
        nodes.push_back(libzp::Node(ip, port));
      }

      s = cluster->RemoveNodes(nodes);
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

      libzp::Cluster::MigrateCmd* cmd = cluster->GetMigrateCmd();
      s = cluster->Expand(table_name, new_nodes, cmd);
      if (!s.ok()) {
        printf("Expand error happened: %s\n", s.ToString().c_str());
        continue;
      }

      cluster->DumpMigrateCmd(cmd);
      printf("Continue? (Y/N)\n");
      char confirm = getchar(); getchar();  // ignore \n
      if (std::tolower(confirm) != 'y') {
        printf("Commmand Aborted\n");
        continue;
      }

      s = cluster->SubmitMigrateCmd();
      if (!s.ok()) {
        printf("SubmitMigrateCmd error happened: %s\n", s.ToString().c_str());
        continue;
      }
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "EXPANDALLTABLE ", 15)) {
      if (line_args.size() < 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::vector<libzp::Node> new_nodes;
      for (size_t i = 1; i< line_args.size(); ++i) {
        std::string ip;
        int port = -1;
        if (!slash::ParseIpPortString(line_args[i], ip, port)) {
          printf("unknow ip:port format, %s\n", line_args[i].c_str());
          continue;
        }
        new_nodes.push_back(libzp::Node(ip, port));
      }

      std::vector<std::string> tables;
      s = cluster->ListTable(&tables);
      if (!s.ok()) {
        std::cout << "ListTable failed: " << s.ToString() << std::endl;
        continue;
      }

      printf("Adding new nodes:\n");
      for (auto& node : new_nodes) {
        printf("   --- %s:%d\n", node.ip.c_str(), node.port);
      }

      libzp::Cluster::MigrateCmd* cmd = cluster->GetMigrateCmd();
      for (auto& table : tables) {
        s = cluster->Expand(table, new_nodes, cmd);
        if (!s.ok()) {
          printf("Expand failed: %s\n", s.ToString().c_str());
          continue;
        }
      }

      cluster->DumpMigrateCmd(cmd);
      printf("Continue? (Y/N)\n");
      char confirm = getchar(); getchar();  // ignore \n
      if (std::tolower(confirm) != 'y') {
        printf("Commmand Aborted\n");
        continue;
      }

      s = cluster->SubmitMigrateCmd();
      if (!s.ok()) {
        printf("SubmitMigrateCmd error happened: %s\n", s.ToString().c_str());
        continue;
      }
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "MIGRATE ", 8)) {
      if (line_args.size() < 5) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      int partition_id = atoi(line_args[3].c_str());
      libzp::Node src_node, dst_node;
      if (!slash::ParseIpPortString(line_args[2], src_node.ip, src_node.port)) {
        printf("unknow source ip:port format, %s\n", line_args[2].c_str());
        continue;
      }
      if (!slash::ParseIpPortString(line_args[4], dst_node.ip, dst_node.port)) {
        printf("unknow destination ip:port format, %s\n", line_args[4].c_str());
        continue;
      }

      libzp::Cluster::MigrateCmd* cmd = cluster->GetMigrateCmd();
      s = cluster->Migrate(table_name, src_node, partition_id, dst_node, cmd);
      if (!s.ok()) {
        printf("Migrate error happened: %s\n", s.ToString().c_str());
        continue;
      }

      cluster->DumpMigrateCmd(cmd);
      printf("Continue? (Y/N)\n");
      char confirm = getchar(); getchar();  // ignore \n
      if (std::tolower(confirm) != 'y') {
        printf("Commmand Aborted\n");
        continue;
      }

      s = cluster->SubmitMigrateCmd();
      if (!s.ok()) {
        printf("SubmitMigrateCmd error happened: %s\n", s.ToString().c_str());
        continue;
      }
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

      libzp::Cluster::MigrateCmd* cmd = cluster->GetMigrateCmd();
      s = cluster->Shrink(table_name, deleting, cmd);
      if (!s.ok()) {
        printf("Shrink error happened: %s\n", s.ToString().c_str());
        continue;
      }

      cluster->DumpMigrateCmd(cmd);
      printf("Continue? (Y/N)\n");
      char confirm = getchar(); getchar();  // ignore \n
      if (std::tolower(confirm) != 'y') {
        printf("Commmand Aborted\n");
        continue;
      }

      s = cluster->SubmitMigrateCmd();
      if (!s.ok()) {
        printf("SubmitMigrateCmd error happened: %s\n", s.ToString().c_str());
        continue;
      }
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "SHRINKALLTABLE ", 15)) {
      if (line_args.size() < 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::vector<libzp::Node> deleting;
      for (size_t i = 1; i< line_args.size(); ++i) {
        std::string ip;
        int port = -1;
        if (!slash::ParseIpPortString(line_args[i], ip, port)) {
          printf("unknow ip:port format, %s\n", line_args[i].c_str());
          continue;
        }
        deleting.push_back(libzp::Node(ip, port));
      }

      std::vector<std::string> tables;
      s = cluster->ListTable(&tables);
      if (!s.ok()) {
        std::cout << "ListTable failed: " << s.ToString() << std::endl;
        continue;
      }
      printf("Deleting nodes:\n");
      for (auto& node : deleting) {
        printf("   --- %s:%d\n", node.ip.c_str(), node.port);
      }

      libzp::Cluster::MigrateCmd* cmd = cluster->GetMigrateCmd();
      for (auto& table : tables) {
        s = cluster->Shrink(table, deleting, cmd);
        if (!s.ok() && !s.IsNotFound()) {
          printf("Shrink failed: %s\n", s.ToString().c_str());
          continue;
        }
      }

      cluster->DumpMigrateCmd(cmd);
      printf("Continue? (Y/N)\n");
      char confirm = getchar(); getchar();  // ignore \n
      if (std::tolower(confirm) != 'y') {
        printf("Commmand Aborted\n");
        continue;
      }

      s = cluster->SubmitMigrateCmd();
      if (!s.ok()) {
        printf("SubmitMigrateCmd error happened: %s\n", s.ToString().c_str());
        continue;
      }
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "REPLACENODE ", 12)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      libzp::Node ori_node, dst_node;
      if (!slash::ParseIpPortString(line_args[1],
                                    ori_node.ip,
                                    ori_node.port)) {
        printf("unknow source(ip:port) format, %s\n", line_args[1].c_str());
        continue;
      }
      if (!slash::ParseIpPortString(line_args[2],
                                    dst_node.ip,
                                    dst_node.port)) {
        printf("unknow destination(ip:port) format, %s\n", line_args[2].c_str());
        continue;
      }

      libzp::Cluster::MigrateCmd* cmd = cluster->GetMigrateCmd();
      s = cluster->ReplaceNode(ori_node, dst_node, cmd);
      if (!s.ok()) {
        printf("ReplaceNode failed: %s\n", s.ToString().c_str());
      }

      cluster->DumpMigrateCmd(cmd);
      printf("Continue? (Y/N)\n");
      char confirm = getchar(); getchar();  // ignore \n
      if (std::tolower(confirm) != 'y') {
        printf("Commmand Aborted\n");
        continue;
      }

      s = cluster->SubmitMigrateCmd();
      if (!s.ok()) {
        printf("SubmitMigrateCmd error happened: %s\n", s.ToString().c_str());
        continue;
      }
      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "CANCELMIGRATE", 13)) {
      if (line_args.size() != 1) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      s = cluster->CancelMigrate();
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

      std::map<libzp::Node, std::string> meta_status;
      libzp::Node leader;
      cluster->MetaStatus(&leader, &meta_status);

      printf("Leader:\n --- %s:%d, status: %s\n",
             leader.ip.c_str(), leader.port, meta_status[leader].c_str());
      printf("Followers:\n");
      for (auto& item : meta_status) {
        const auto& node = item.first;
        if (node == leader) {
          continue;
        } else {
          printf(" --- %s:%d, status: %s\n",
                 node.ip.c_str(), node.port, meta_status[node].c_str());
        }
      }

      std::cout << s.ToString() << std::endl;
    } else if (!strncasecmp(line, "METASTATUS", 10)) {
      if (line_args.size() != 1) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      int32_t version;
      std::string consistency_stautus;
      int64_t begin_time;
      int32_t complete_proportion;
      s = cluster->MetaStatus(&version, &consistency_stautus,
                              &begin_time, &complete_proportion);
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
        continue;
      }
      std::cout << "Epoch: " << version << std::endl;
      std::cout << "Consistency status: " << std::endl;
      std::cout << std::string(140, '=') << std::endl;
      std::cout << consistency_stautus << std::endl;
      std::cout << std::string(140, '=') << std::endl;
      if (begin_time != 0) {
        std::cout << "Migrate Status:" << std::endl;
        std::cout << "begin_time: " << begin_time << std::endl;
        std::cout << "complete_proportion: " << complete_proportion << std::endl;
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

    } else if (!strncasecmp(line, "LATENCY", 7)) {
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::map<libzp::Node, std::string> latency_info;
      s = cluster->InfoLatency(table_name, &latency_info);
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
        continue;
      }
      printf("Table %s latency:\n", table_name.c_str());
      for (auto& item : latency_info) {
        printf("On node: %s\n", item.first.ToString().c_str());
        printf("%s\n", item.second.c_str());
      }
    } else if (!strncasecmp(line, "REPLSTATE", 9)) {
      if (line_args.size() != 3) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string table_name = line_args[1];
      std::string ip;
      int port;
      slash::ParseIpPortString(line_args[2], ip, port);
      libzp::Node node(ip, port);
      std::map<int, libzp::PartitionView> partitions;
      libzp::Status s = cluster->InfoRepl(node, table_name, &partitions);
      if (!s.ok()) {
        std::cout << "Failed: " << s.ToString() << std::endl;
      }
      for (auto& p : partitions) {
        printf("partition: %d, role: %s, repl_state: %s, boffset: %u_%lu\n",
               p.first, p.second.role.c_str(), p.second.repl_state.c_str(),
               p.second.offset.filenum, p.second.offset.offset);
        printf("  --- master: %s:%d\t", p.second.master.ip.c_str(), p.second.master.port);
        for (auto& pss : p.second.slaves) {
          printf("slave: %s:%d\t", pss.ip.c_str(), pss.port);
        }
        printf("\n");

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
      if (line_args.size() != 2) {
        std::cout << "arg num wrong" << std::endl;
        continue;
      }
      std::string ip;
      int port;
      slash::ParseIpPortString(line_args[1], ip, port);
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
    } else if (!strncasecmp(line, "HELP", 4)) {
      for (auto& entry : commandEntries) {
        std::cout << entry.name << ": " << entry.params << std::endl;
        std::cout << " --- " << entry.info << std::endl;
        std::cout << std::endl;
      }
    } else {
      printf("Unrecognized command: %s\n", line);
    }
    free(line);
  }
  std::cout << "Bye!" << std::endl;
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

  libzp::Options option;
  const char* ip = argv[1];
  int port = std::atoi(argv[2]);
  option.meta_addr.push_back(libzp::Node(ip, port));
  option.op_timeout = 5000;

  // cluster handle cluster operation
  libzp::Cluster* cluster = new libzp::Cluster(option);

  cliInitHelp();
  StartRepl(cluster, ip, port);
}
