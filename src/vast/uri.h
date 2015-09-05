#ifndef VAST_URI_H
#define VAST_URI_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <map>

namespace vast {

struct uri {
  std::string protocol;
  std::string hostname;
  uint16_t port;
  std::vector<std::string> path;
  std::map<std::string,std::string> options;
  std::string fragment;
};

} // namespace vast

#endif
