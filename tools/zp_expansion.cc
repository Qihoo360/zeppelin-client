#include <string>
#include <vector>
#include <iostream>

#include "slash/include/slash_status.h"
#include "libzp/include/zp_cluster.h"


void usage() {
  std::cout << "usage:\n"
            << "      zp_expansion meta_host meta_port table [ADD/REMOVE] host port\n";
}

void error(const std::string& msg) {
  std::cout << msg << std::endl;
  usage();
  exit(-1);
}

int main(int argc, char* argv[]) {
  if (argc < 7) {
    error("Too few arguments");
  }

  libzp::Options option;
  libzp::Node meta_node(argv[1], atoi(argv[2]));
  option.meta_addr.push_back(meta_node);

  std::string table = argv[3];
  std::string type = argv[4];
  if (type != "ADD" && type != "REMOVE") {
    error("type only can be ADD or REMOVE");
  }

  std::vector<libzp::Node> target_nodes;
  for (int i = 5; i < argc; ++i) {
    target_nodes.push_back(libzp::Node(argv[4], atoi(argv[i])));
  }
  if (target_nodes.empty()) {
    error("No target node");
    return -1;
  }

  std::cout << "Expending cluster(" << meta_node << ")." << std::endl;
  std::cout << type << "nodes: " << std::endl;
  for (auto& n : target_nodes) {
    std::cout << " " << n << std::endl;
  }
  std::cout << "Confirm? (y or n) : ";
  char confirm;
  std::cin >> confirm;
  if (confirm != 'y') {
    return 0;
  }

  std::cout << "Create cluster, " << meta_node << std::endl;
  libzp::Cluster *cluster = new libzp::Cluster(option);
  assert(cluster);

  std::cout << "Connect to meta" << std::endl;
  slash::Status s = cluster->Connect();
  if (!s.ok()) {
    std::cout << "Connect failed: " << s.ToString() << std::endl;
    return -1;
  }

  std::cout << "Pull meta info from meta node" << std::endl;
  libzp::Table table_info;
  s = cluster->FetchMetaInfo(table, &table_info);
  if (!s.ok()) {
    std::cout << "Failed to pull table: " << table << s.ToString() << std::endl;
    return -1;
  }

  std::cout << "Check target node" << std::endl;
  std::set<libzp::Node> cur_nodes, master_nodes;
  table_info.GetAllNodes(&cur_nodes);
  table_info.GetAllMasters(&master_nodes);
  for (auto& target : target_nodes) {
    if (cur_nodes.find(target) != cur_nodes.end() && type == "ADD") {
      std::cout << "ADD Target node " << target << " already in cluster." << std::endl;
      return -1;
    } else if (cur_nodes.find(target) == cur_nodes.end() && type == "REMOVE") {
      std::cout << "REMOVE Target node " << target << " is not in cluster." << std::endl;
      return -1;
    } else if (master_nodes.find(target) != master_nodes.end()) {
      std::cout << "REMOVE Target node " << target << " is master in cluster." << std::endl;
      return -1;
    }
  }

  if (type == "ADD") {
    std::cout << "Begin ADD nodes into cluster" << std::endl;
    for (int i = 0, ti = 0; i < table_info.partition_num(); ++i) {
      s = cluster->AddSlave(table, i, target_nodes[ti]);
      std::cout << (s.ok() ? "Success" : "Failed")
        << " to add slave(" << target_nodes[ti] << ") into table:" << table
        << ", partition:" << i << " " << (s.ok() ? "" : s.ToString());
      ti = (ti + 1) % target_nodes.size();
    }
    std::cout << "Finish ADD nodes into cluster" << std::endl;
  } else {
    std::cout << "Begin REMOVE nodes into cluster" << std::endl;
    std::set<libzp::Node> target_set;
    for (auto t : target_nodes) {
      target_set.insert(t);
    }
    for (int i = 0; i < table_info.partition_num(); ++i) {
      const libzp::Partition* part = table_info.GetPartitionById(i);
      assert(part);
      std::vector<libzp::Node> slaves = part->slaves();
      int slave_num = slaves.size();
      for (auto& slave : slaves) {
        auto iter = target_set.find(slave);
        if (iter != target_set.end()) {
          if (slave_num <= 1) {
            std::cout << "REMOVE Target node " << *iter
              << "is the last one of table:" << table << ", partition:" << i << std::endl;  
            return -1;
          }
          s = cluster->RemoveSlave(table, i, *iter);
          slave_num--;
          std::cout << (s.ok() ? "Success" : "Failed")
            << " to remove slave(" << *iter << ") from table:"<< table
            << ", partition:" << i << " " << (s.ok() ? "" : s.ToString());
        }
      }
    }
    std::cout << "Finish REMOVE nodes from cluster" << std::endl;
  }

  delete cluster;
}
