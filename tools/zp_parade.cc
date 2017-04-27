#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "slash/include/env.h"
#include "slash/include/slash_status.h"
#include "libzp/include/zp_cluster.h"


void usage() {
  std::cout << "usage:\n"
            << "      zp_parade host port [count_base]\n";
}

enum Op {
  kSet = 0,
  kGet,
  kDelete,
  kMget,
};

void SingleKeyOp(libzp::Client* client, int count, int vlen, Op op) {
  uint64_t start_time = 0, end_time = 0;
  std::string value(vlen, 'a'), slen = std::to_string(vlen);
  slash::Status s;
  std::cout << "Begin Op " << count << " items ..." << std::endl;
  start_time = slash::NowMicros();
  for(int i = 0; i < count; ++i) {
    std::string si = std::to_string(i);
    std::string key = "key_" + slen + "_" + si;
    switch (op) {
      case kSet:
        s = client->Set(key, value + si);
        break;
      case kGet:
        s = client->Get(key, &value);
        break;
      case kDelete:
        s = client->Delete(key);
        break;
      default:
        break;
    }
    if (!s.ok()) {
      std::cout << "[ERROR]Op failed, len" << vlen
        << ", " << s.ToString() << std::endl;
      return;
    }
  }
  end_time = slash::NowMicros();
  std::cout << "Success Op" << count << " items, time(us): "
    << (end_time - start_time) << std::endl;
  
  std::cout << std::endl;
}

void MultiKeyOp(libzp::Client* client, int count, int vlen, Op op) {
  uint64_t start_time = 0, end_time = 0;
  std::string slen = std::to_string(vlen);
  slash::Status s;
  
  std::string key;
  std::vector<std::string> keys;
  std::map<std::string, std::string> values;
  for(int i = 0; i < count; ++i) {
    key = "key_" + slen + "_" + std::to_string(i);
    keys.push_back(key);
  }

  std::cout << "Begin Op " << count << " items ..." << std::endl;
  start_time = slash::NowMicros();
  switch (op) {
    case kMget:
      s = client->Mget(keys, &values);
      break;
    default:
      break;
  }
  if (!s.ok()) {
    std::cout << "[ERROR]Op failed, len" << s.ToString() << std::endl;
    return;
  }
  end_time = slash::NowMicros();
  std::cout << "Success Op" << count << " items, time(us): "
    << (end_time - start_time) << std::endl;

  //for (auto& v : values) {
  //  std::cout << v.first << "->" << v.second << std::endl;
  //}
  std::cout << std::endl;
}

int main(int argc, char* argv[]) {
  if (argc != 3 && argc != 4) {
    usage();
    return -1;
  }

  std::cout << "Create client" << std::endl;
  libzp::Client* client = new libzp::Client(argv[1], atoi(argv[2]), "parade");
  
  std::cout << "Connect cluster" << std::endl;
  slash::Status s = client->Connect();
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return -1;
  }
  
  int count_base = 1;
  if (argc == 4) {
    count_base = atoi(argv[3]);
  }

  std::cout << "--------- SET " << 100 * count_base << " 10B Value ----------" << std::endl;
  SingleKeyOp(client, 100 * count_base, 10, Op::kSet);

  std::cout << "--------- SET " << 100 * count_base << " 100B Value ----------" << std::endl;
  SingleKeyOp(client, 100 * count_base, 100, Op::kSet);

  std::cout << "--------- SET " << 10 * count_base << " 1KB Value ----------" << std::endl;
  SingleKeyOp(client, 10 * count_base, 1000, Op::kSet);

  std::cout << "--------- GET " << 100 * count_base << " 10B Value ----------" << std::endl;
  SingleKeyOp(client, 100 * count_base, 10, Op::kGet);

  std::cout << "--------- GET " << 100 * count_base << " 100B Value ----------" << std::endl;
  SingleKeyOp(client, 100 * count_base, 100, Op::kGet);

  std::cout << "--------- GET " << 10 * count_base << " 1KB Value ----------" << std::endl;
  SingleKeyOp(client, 10 * count_base, 1000, Op::kGet);
  
  std::cout << "--------- MGET " << 100 * count_base << " 10B Value ----------" << std::endl;
  MultiKeyOp(client, 100 * count_base, 10, Op::kMget);

  std::cout << "--------- MGET " << 100 * count_base << " 100B Value ----------" << std::endl;
  MultiKeyOp(client, 100 * count_base, 100, Op::kMget);
  
  std::cout << "--------- MGET " << 10 * count_base << " 1KB Value ----------" << std::endl;
  MultiKeyOp(client, 10 * count_base, 1000, Op::kMget);

  std::cout << "--------- DELETE " << 100 * count_base << " 10B Value ----------" << std::endl;
  SingleKeyOp(client, 100 * count_base, 10, Op::kDelete);

  std::cout << "--------- DELETE " << 100 * count_base << " 100B Value ----------" << std::endl;
  SingleKeyOp(client, 100 * count_base, 100, Op::kDelete);

  std::cout << "--------- DELETE " << 10 * count_base << " 1KB Value ----------" << std::endl;
  SingleKeyOp(client, 10 * count_base, 1000, Op::kDelete);
  
  delete client;
}
