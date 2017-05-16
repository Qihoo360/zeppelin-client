/*
 * "Copyright [2016] qihoo"
 */
#include <unistd.h>

#include <string>
#include <vector>
#include <iostream>

#include "libzp/include/zp_client.h"


void usage() {
  std::cout << "usage:\n"
            << "      zp_async host port\n";
}

static std::string user_data = "ASYNC";

void set_callback(const struct libzp::Result& stat, void* data) {
  std::cout << "set " << stat.key << "callback called. Result: "<< stat.ret.ToString()
    << ", usr data: "<< *(static_cast<std::string*>(data)) << std::endl;
}

void get_callback(const struct libzp::Result& stat, void* data) {
  std::cout << "get " << stat.key << " callback called. Result: "<< stat.ret.ToString()
    << ", value:" << *(stat.value)
    << ", usr data: "<< *(static_cast<std::string*>(data)) << std::endl;
}

void delete_callback(const struct libzp::Result& stat, void* data) {
  std::cout << "delete " << stat.key  << " callback called. Result: "<< stat.ret.ToString()
    << ", usr data: "<< *(static_cast<std::string*>(data)) << std::endl;
}

void mget_callback(const struct libzp::Result& stat, void* data) {
  std::cout << "mget callback called. Result: "<< stat.ret.ToString() << std::endl;
  for (auto& get : *stat.kvs) {
    std::cout << get.first << " <-> " << get.second << std::endl;
  }
  std::cout << "usr data: "<< *(static_cast<std::string*>(data)) << std::endl;
}

int main(int argc, char* argv[]) {
  if (argc != 4) {
    usage();
    return -1;
  }

  // client handle io operation
  std::cout << "create client" << std::endl;
  libzp::Client* client = new libzp::Client(argv[1], atoi(argv[2]), "parade");
  std::cout << "connect cluster" << std::endl;
  int cnt = atoi(argv[3]);

  std::cout << "###############Aset#################" << std::endl;
  for(int i = 0; i < cnt; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value(30, 'a');
    client->Aset(key, value, set_callback, &user_data);
  }
  std::cout << std::endl;


  //sleep(1);
  //std::map<std::string, std::string> kvs;
  //for(int i = 0; i < cnt; i = i + 10) {
  //  kvs.clear();
  //  keys.clear();
  //  for (int j = 0; j < 10; ++j) {
  //    std::string key = "key" + std::to_string(i + j);
  //    keys.push_back(key);
  //  }
  //  client->Mget(keys, &kvs);
  //  for (auto& kv : kvs) {
  //    std::cout << kv.first << " <-> " << kv.second << std::endl;
  //  }
  //}


  for(int i = 0; i < cnt; i++) {
    std::string key = "key" + std::to_string(i);
    client->Aget(key, get_callback, &user_data);
  }

  std::vector<std::string> keys;
  for(int i = 0; i < cnt; i = i + 10) {
    keys.clear();
    for (int j = 0; j < 10; ++j) {
      std::string key = "key" + std::to_string(i + j);
      keys.push_back(key);
    }
    client->Amget(keys, mget_callback, &user_data);
  }

  for(int i = 0; i < cnt; i++) {
    std::string key = "key" + std::to_string(i);
    client->Adelete(key, delete_callback, &user_data);
  }
  std::cout << std::endl;

  // wait async process finished
  sleep(10);


  delete client;
}
