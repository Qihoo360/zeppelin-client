/*
 * "Copyright [2016] qihoo"
 */
#include <unistd.h>

#include <string>
#include <iostream>

#include "slash/include/slash_status.h"

#include "libzp/include/zp_client.h"


void usage() {
  std::cout << "usage:\n"
            << "      zp_get host port table key\n";
}

int main(int argc, char* argv[]) {
  if (argc != 5) {
    usage();
    return -1;
  }

  // client handle io operation
  std::cout << "create client" << std::endl;
  libzp::Client* client = new libzp::Client(argv[1], atoi(argv[2]), argv[3]);
  libzp::Status s;
  
  std::string value;
  s = client->Get(argv[4], &value);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  } else {
    std::cout << "get ok, value: " << value << std::endl;
  }

  delete client;
  return 1;
}
