#include <cassert>
#include "rocksdb/db.h"

#include "slash/include/env.h"
#include "libzp/include/zp_cluster.h"

void usage() {
  std::cout << "usage:\n"
    << "      ./rocksdb_to_zp db_path meta_ip meta_port table_name\n";
}

int main(int argc, char* argv[]) {
  if (argc != 5) {
    usage();
    return -1;
  }
  std::string db_path(argv[1]);
  std::string meta_ip(argv[2]);
  int meta_port = atoi(argv[3]);
  std::string table_name(argv[4]);

  if (!slash::FileExists(db_path)) {
    std::cout << "Db not exist: " << db_path << std::endl;
    usage();
    return -1;
  }

  // Connect zp
  libzp::Options option;
  libzp::Node node(meta_ip, meta_port);
  option.meta_addr.push_back(node);
  libzp::Cluster *cluster = new libzp::Cluster(option);
  libzp::Status s = cluster->Connect();
  if (!s.ok()) {
    std::cout << "connect meta server failed! meta_ip: " << meta_ip
      << ", meta_port: " << meta_port
      << ", Error: "<< s.ToString() << std::endl;
    delete cluster;
    return -1;
  }

  // Open rocksdb
  rocksdb::DB* db;
  rocksdb::Options options;
  rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
  if (!status.ok()) {
    std::cout << "Failed to open db: " << db_path
      << ", error: " << status.ToString() << std::endl;
    delete cluster;
    return -1;
  }

  std::cout << "Import data from rocksdb, db:" << db_path
    << ", meta_ip: " << meta_ip << ", meta_port: " << meta_port
    << ", table_name: " << table_name << ". y or n?"<< std::endl;
  char c;
  std::cin >> c;
  if (c != 'y') {
    delete cluster;
    delete db;
    return -1;
  }

  // Iterator all key value and write into zp
  int succ_num = 0, fail_num = 0;
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    s = cluster->Set(table_name, it->key().ToString(), it->value().ToString());
    if (!s.ok()) {
      std::cout << "set key " << it->key().ToString() << " failed: "
        << s.ToString() << std::endl;
      fail_num++;
      continue;
    }
    succ_num++;
    std::cout << "set key " << it->key().ToString() << " success." << std::endl;
  }
  std::cout << "Finished. succ: " << succ_num
    << ", fail: " << fail_num << std::endl;
  
  delete it;
  delete db;
  delete cluster;
  return 0;
}

