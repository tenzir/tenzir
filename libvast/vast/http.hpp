#ifndef VAST_HTTP_HPP
#define VAST_HTTP_HPP

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "vast/uri.hpp"

namespace vast {
namespace http {

struct header {
  std::string name;
  std::string value;
};

/// Base for HTTP messages.
struct message {
  std::string protocol;
  double version;
  std::vector<header> headers;
  std::string body;

  header const* header(std::string const& name) const;
};

/// A HTTP request message.
struct request : message {
  std::string method;
  vast::uri uri;
};

/// A HTTP response message.
struct response : message {
  uint32_t status_code;
  std::string status_text;
};

} // namespace http
} // namespace vast

#endif
