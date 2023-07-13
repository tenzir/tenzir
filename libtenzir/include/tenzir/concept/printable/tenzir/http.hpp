//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/numeric/integral.hpp"
#include "tenzir/concept/printable/numeric/real.hpp"
#include "tenzir/concept/printable/string/any.hpp"
#include "tenzir/concept/printable/string/string.hpp"
#include "tenzir/http.hpp"

#include <string>

namespace tenzir {

struct http_header_printer : printer_base<http_header_printer> {
  using attribute = http::header;

  template <class Iterator>
  bool print(Iterator& out, const http::header& hdr) const {
    using namespace printers;
    auto p = str << ": " << str;
    return p(out, hdr.name, hdr.value);
  }
};

template <>
struct printer_registry<http::header> {
  using type = http_header_printer;
};

struct http_response_printer : printer_base<http::response> {
  using attribute = http::response;

  template <class Iterator>
  bool print(Iterator& out, const http::response& res) const {
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

} // namespace tenzir
