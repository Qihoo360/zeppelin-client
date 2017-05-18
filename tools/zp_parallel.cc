/*
 * "Copyright [2016] qihoo"
 */

#include <iostream>
#include <cstdlib>

#include "libzp/include/zp_cluster.h"
#include "libzp/include/zp_client.h"

static std::string user_data = "ASYNC";

libzp::Client* client_ptr = NULL;
libzp::Cluster* cluster_ptr = NULL;
libzp::Status s;

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


void AsyncSet() {
  for(int i = 0; i < 10; i++) {
    std::string key = "async_key" + std::to_string(i);
    std::string value(30, 'a');
    client_ptr->Aset(key, value, set_callback, &user_data);
  }
}

void AsyncGet() {
  for(int i = 0; i < 10; i++) {
    std::string key = "async_key" + std::to_string(i);
    client_ptr->Aget(key, get_callback, &user_data);
  }
}

void AsyncMget() {
  std::vector<std::string> keys;
  for(int i = 0; i < 10; i = i + 10) {
    keys.clear();
    for (int j = 0; j < 10; ++j) {
      std::string key = "async_key" + std::to_string(i + j);
      keys.push_back(key);
    }
    client_ptr->Amget(keys, mget_callback, &user_data);
  }
}

void AsyncDelete() {
  for(int i = 0; i < 10; i++) {
    std::string key = "async_key" + std::to_string(i);
    client_ptr->Adelete(key, delete_callback, &user_data);
  }
}

void Set() {
  libzp::Status s;
  for(int i = 0; i < 10; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value(30, 'a');
    s = client_ptr->Set(key, value);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
    } else {
      std::cout << "Set key:" << key << " value: " << value << std::endl;
    }
  }
}

void Get() {
  for(int i = 0; i < 10; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value;
    s = client_ptr->Get(key, &value);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
    } else {
      std::cout << "Get key:" << key << " value: " << value << std::endl;
    }
  }
}

void Mget() {
  std::vector<std::string> keys;
  std::map<std::string, std::string> kvs;
  for(int i = 0; i < 10; i = i + 10) {
    keys.clear();
    kvs.clear();
    for (int j = 0; j < 10; ++j) {
      std::string key = "key" + std::to_string(i + j);
      keys.push_back(key);
    }
    client_ptr->Mget(keys, &kvs);
  }
}

void Delete() {
  for(int i = 0; i < 10; i++) {
    std::string key = "key" + std::to_string(i);
    s = client_ptr->Delete(key);
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
    } else {
      std::cout << "Delete key:" << key << std::endl;
    }
  }
}

void CreateTable() {
  for(int i = 0; i < 10; i++) {
    std::string table = "table" + std::to_string(i);    
    s = cluster_ptr->CreateTable(table, 3);
    if (!s.ok()) {
      std::cout << "Create table " << table << " failed, " << s.ToString() << std::endl;
      continue;
    } else {
      std::cout << "Create table " << table << " success" << std::endl;
    }
  }
}

void DropTable() {
  for(int i = 0; i < 10; i++) {
    std::string table = "table" + std::to_string(i);    
    s = cluster_ptr->DropTable(table);
    if (!s.ok()) {
      std::cout << "Drop table " << table << " failed, " << s.ToString() << std::endl;
      continue;
    } else {
      std::cout << "Drop table " << table << " success" << std::endl;
    }
  }
}

void Pull() {
  for(int i = 0; i < 10; i++) {
    std::string table = "table" + std::to_string(i);    
    s = cluster_ptr->Pull(table);
    if (!s.ok()) {
      std::cout << "Pull table " << table << " failed, " << s.ToString() << std::endl;
      continue;
    } else {
      std::cout << "Pull table " << table << " success" << std::endl;
    }
  }
}


void InfoQps() {
  for(int i = 0; i < 10; i++) {
    std::string table = "table" + std::to_string(i);
    int qps, total_query;
    s = cluster_ptr->InfoQps(table, &qps, &total_query);
    if (!s.ok()) {
      std::cout << "InfoQps " << table << " failed, " << s.ToString() << std::endl;
      continue;
    } else {
      std::cout << table << " QPS: " << qps << ", TOTAL_QUERY: " << total_query << std::endl;
    }
  }
}

int main(int argc, char* argv[]) {

  // connect to cluster
  libzp::Cluster cluster_obj(argv[1], atoi(argv[2]));
  cluster_ptr = &cluster_obj;
  s = cluster_ptr->Connect();
  if (!s.ok()) {
      std::cout << "Connect cluster failed\n";
      return -1;
  }
  
  s = cluster_ptr->CreateTable("parallel", 3);
  if (!s.ok()) {
    std::cout << "Create table parallel failed: " << s.ToString() << std::endl;
    //return -1;
  } else {
    std::cout << "Create table parallel success" << std::endl;
  }
  
  // connect to client
  libzp::Client client_obj(argv[1], atoi(argv[2]), "parallel");
  client_ptr = &client_obj;
  s = client_ptr->Connect();
  if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      return -1;
  }
 

  for(int i = 0; i < 100; i++) {
    int random = rand() % 11;
    switch(random) {
      // Asynchronous command
      case 0:
        AsyncSet();
        break;
      case 1:
        AsyncGet();
        break;
      case 2:
        AsyncMget();
        break;
      case 3:
        AsyncDelete();
        break;
      // Data command
      case 4:
        Set();
        break;
      case 5:
        Get();
        break;
      case 6:
        Mget();
        break;
      case 7:
        Delete();
      // Meta command
      case 8:
        CreateTable();
        break;
      case 9:
        DropTable();
        break;
      case 10:
        Pull();
        break;
      // Statistical cmd
      case 11:
        InfoQps();
        break;
    }
  }

  delete cluster_ptr;
  delete client_ptr;
 }