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
            << "      ./zp_benchmark host port table clientnum requrestnum\n";
}

void Run(const libzp::Options &option,
    const std::string &table, const std::string &prefix, int rnum) {
  libzp::Cluster *cluster = new libzp::Cluster(option);
  libzp::Status s = cluster->Connect();
  if (!s.ok()) {
    std::cout << "client " << std::this_thread::get_id()
      << " connect server failed: " << s.ToString() << std::endl;
    delete cluster;
    return;
  }
  s = cluster->Pull(table);
  if (!s.ok()) {
    std::cout << "client " << std::this_thread::get_id()
      << " pull table " << table << " failed: " << s.ToString() << std::endl;
    delete cluster;
    return;
  }

  // Set
  std::string key;
  std::string value = "value";
  for (int i = 0; i < rnum; i++) {
    key = prefix + std::to_string(i);
    s= cluster->Set(table, key, value);
    if (!s.ok()) {
      std::cout << "client " << std::this_thread::get_id() << " set key "
        << key << " failed: " << s.ToString() << std::endl;
      delete cluster;
      return;
    }
  }

  // Get
  std::string v;
  for (int i = 0; i < rnum; i++) {
    key = prefix + std::to_string(i);
    s= cluster->Get(table, key, &v);
    if (!s.ok()) {
      std::cout << "client " << std::this_thread::get_id()
        << " get key " << key << " failed: " << s.ToString() << std::endl;
      delete cluster;
      return;
    }
  }

  // Mget
  std::map<std::string, std::string> kvs;
  for (int i = 0; i < rnum; i+=10) {
    std::vector<std::string> keys;
    for (int j = 10; j > 0; --j) {
      keys.push_back(prefix + std::to_string(i + j));
    }
    s = cluster->Mget(table, keys, &kvs);
    if (!s.ok()) {
      std::cout << "client " << std::this_thread::get_id()
        << " mget 10 key start with " << keys[0] << " failed: " << s.ToString() << std::endl;
      delete cluster;
      return;
    }
  }
  std::cout << "client " << std::this_thread::get_id() << " is done! "
    << "Processed " <<rnum << "Set and " << rnum << "Get"  << rnum << "Mget"<< std::endl;
} 

int main(int argc, char* argv[]) {
  if (argc != 6) {
    usage();
    return -1;
  }
  std::string host = argv[1];
  int port = atoi(argv[2]);
  std::string table = argv[3];
  int client_num = atoi(argv[4]);
  int request_num = atoi(argv[5]);

  libzp::Options option;
  libzp::Node node(host, port);
  option.meta_addr.push_back(node);

  std::thread *threads = new std::thread[client_num];

  for (int i = 0; i < client_num; i++) {
    threads[i] = std::thread(Run, option, table, std::to_string(i), request_num/client_num);
  }

  for (int i = 0; i< client_num; i++) {
    threads[i].join();
  }

  delete[] threads;

  std::cout << "Bye!!" << std::endl;

}
