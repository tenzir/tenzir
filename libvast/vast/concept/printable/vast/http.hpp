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

#ifndef VAST_CONCEPT_PRINTABLE_VAST_HTTP_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_HTTP_HPP

#include <string>

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/numeric/real.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/http.hpp"

namespace vast {

struct http_header_printer : printer<http_header_printer> {
  using attribute = http::header;

  template <typename Iterator>
  bool print(Iterator& out, http::header const& hdr) const {
    using namespace printers;
    auto p = str << ": " << str;
    return p(out, hdr.name, hdr.value);
  }
};

template <>
struct printer_registry<http::header> {
  using type = http_header_printer;
};

struct http_response_printer : printer<http::response> {
  using attribute = http::response;

  template <typename Iterator>
  bool print(Iterator& out, http::response const& res) const {
    using namespace printers;
    auto version = real_printer<double, 1>{};
    auto p 
      =   str     // proto
      << '/' 
      << version 
      << ' ' 
      << u32      // status code
      << ' ' 
      << str      // status text
      << "\r\n"
      << ~(http_header_printer{} % "\r\n")
      << "\r\n\r\n"
      << str      // body
      ;
    return p(out, res.protocol, res.version, res.status_code, res.status_text,
             res.headers, res.body);
  }
};

template <>
struct printer_registry<http::response> {
  using type = http_response_printer;
};

} // namespace vast

#endif
