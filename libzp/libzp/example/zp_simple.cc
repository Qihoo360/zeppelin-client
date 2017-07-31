#include <string>
#include <iostream>
#include "libzp/include/zp_client.h"
void usage() {
  std::cout << "usage:\n"
            << "      zp_timeout host port table timeout\n";
}

int main(int argc, char* argv[]) {
  if (argc != 5) {
    usage();
    return -1;
  }

  // Create client with ip, port and table name, the table must be already exist, you may create table by zp_manager first
  libzp::Options opt;
  opt.op_timeout = atoi(argv[4]);
  opt.meta_addr.push_back(libzp::Node(argv[1], atoi(argv[2])));

  libzp::Client* client = new libzp::Client(opt, argv[3]);
  assert(client);
  // Set key value
  libzp::Status s = client->Set("key", "value");
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  } else {
    std::cout << "set ok" << std::endl;
  }
  
  // Get value by key
  std::string var;
  s = client->Get("key", &var);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  } else {
    std::cout << "get " << var << std::endl;
  }
  
  // Delete key
  s = client->Delete("key");
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  } else {
    std::cout << "delete key" << std::endl;
  }
  
  // Remember to delete the client
  delete client;
  return 0;
}
