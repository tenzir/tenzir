#ifndef VAST_ENDPOINT_H
#define VAST_ENDPOINT_H

#include <string>

namespace vast {

/// A transport-layer endpoint consisting of host and port.
struct endpoint {
  std::string host;   ///< The hostname or IP address.
  uint64_t port = 0;  ///< The transport-layer port.
};

} // namespace vast

#endif
