/*
 * "Copyright [2016] qihoo"
 */
#include <unistd.h>
#include <string>
#include <vector>
#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>

#include "libzp/include/zp_cluster.h"


void usage() {
  std::cout << "usage:\n"
            << "      ./zp_loopset host port table key value_len requrestnum\n";
}

int main(int argc, char* argv[]) {
  if (argc != 7) {
    usage();
    return -1;
  }
  std::string host = argv[1];
  int port = atoi(argv[2]);
  std::string table = argv[3];
  std::string key = argv[4];
  int value_len = atoi(argv[5]);
  int rnum = atoi(argv[6]);

  libzp::Options option;
  libzp::Node node(host, port);
  option.meta_addr.push_back(node);
  
  libzp::Cluster *cluster = new libzp::Cluster(option);
  libzp::Status s = cluster->Connect();
  if (!s.ok()) {
    std::cout << "client " << std::this_thread::get_id()
      << " connect server failed: " << s.ToString() << std::endl;
    delete cluster;
    return -1;
  }
  s = cluster->Pull(table);
  if (!s.ok()) {
    std::cout << "client " << std::this_thread::get_id()
      << " pull table " << table << " failed: " << s.ToString() << std::endl;
    delete cluster;
    return -1;
  }

  // Set
  std::string* value = new std::string(value_len, 'a');
  for (int i = 0; i < rnum; i++) {
    s= cluster->Set(table, key, *value);
    if (!s.ok()) {
      std::cout << "client " << std::this_thread::get_id() << " set key "
        << key << " failed: " << s.ToString() << std::endl;
      delete value;
      delete cluster;
      return -1;
    }
  }

  delete value;
  delete cluster;
  std::cout << "Bye!!" << std::endl;
  return 0;
}
