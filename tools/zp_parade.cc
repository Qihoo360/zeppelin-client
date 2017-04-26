#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "slash/include/env.h"
#include "slash/include/slash_status.h"
#include "libzp/include/zp_cluster.h"


void usage() {
  std::cout << "usage:\n"
            << "      zp_parade host port\n";
}

enum SingleKeyOp {
  kSet = 0,
  kGet,
  kDelete,
};

void SingleOp(libzp::Client* client, int count, int vlen, SingleKeyOp op) {
  uint64_t start_time = 0, end_time = 0;
  std::string value(vlen, 'a'), slen = std::to_string(vlen);
  slash::Status s;
  std::cout << "Begin Op " << count << " items ..." << std::endl;
  start_time = slash::NowMicros();
  for(int i = 0; i < count; i++) {
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
      std::cout << "Set item failed, len" << vlen
        << ", " << s.ToString() << std::endl;
      return;
    }
  }
  end_time = slash::NowMicros();
  std::cout << "Success Op" << count << " items, time(us): "
    << (end_time - start_time) << std::endl;
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
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

  std::cout << "--------- SET 100 10B Value ----------" << std::endl;
  SingleOp(client, 100, 10, SingleKeyOp::kSet);

  std::cout << "--------- SET 100 100B Value ----------" << std::endl;
  SingleOp(client, 100, 100, SingleKeyOp::kSet);

  std::cout << "--------- SET 10 1KB Value ----------" << std::endl;
  SingleOp(client, 10, 1000, SingleKeyOp::kSet);

  std::cout << "--------- GET 100 10B Value ----------" << std::endl;
  SingleOp(client, 100, 10, SingleKeyOp::kGet);

  std::cout << "--------- GET 100 100B Value ----------" << std::endl;
  SingleOp(client, 100, 100, SingleKeyOp::kGet);

  std::cout << "--------- GET 10 1KB Value ----------" << std::endl;
  SingleOp(client, 10, 1000, SingleKeyOp::kGet);

  std::cout << "--------- DELETE 100 10B Value ----------" << std::endl;
  SingleOp(client, 100, 10, SingleKeyOp::kDelete);

  std::cout << "--------- DELETE 100 100B Value ----------" << std::endl;
  SingleOp(client, 100, 100, SingleKeyOp::kDelete);

  std::cout << "--------- DELETE 10 1KB Value ----------" << std::endl;
  SingleOp(client, 10, 1000, SingleKeyOp::kDelete);

  delete client;
}
