//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
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
  using attribute = http::Header;

  template <class Iterator>
  bool print(Iterator& out, const http::Header& hdr) const {
    using namespace printers;
    auto p = str << ": " << str;
    return p(out, hdr.name, hdr.value);
  }
};

template <>
struct printer_registry<http::Header> {
  using type = http_header_printer;
};

} // namespace tenzir
