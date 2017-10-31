// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef DISTRIBUTION_H_
#define DISTRIBUTION_H_

#include <fstream>
#include <iostream>
#include <vector>
#include <deque>
#include <map>
#include <cstdlib>
#include <ctime>

namespace distribution {

struct Host {
  // ip:port
  std::string host;
  // host id
  int host_id;
  // cabinet id
  int cab_id;
};

extern std::vector<std::vector<Host> > result;

bool Load(const std::string& file);
void Distribution();
void Checkup();
void Cleanup();

}  // namespace distribution

#endif
