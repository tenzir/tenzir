/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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
  std::vector<http::header> headers;
  std::string body;

  http::header const* header(std::string const& name) const;
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
