#ifndef VAST_UTIL_ENDPOINT_H
#define VAST_UTIL_ENDPOINT_H

#include <string>

namespace vast {
namespace util {

// Parses an endpoint of the form `(ipv4|name)(:port)?|[ipv6]:port|ipv6`.
// @param *host* The result parameter containing the host component.
// @param *port* The result parameter containing the port component.
// @returns `true` on success.
bool parse_endpoint(std::string const& str, std::string& host, uint16_t& port);

} // namespace util
} // namespace vast

#endif
