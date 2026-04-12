//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/detail/print_delimited.hpp"
#include "tenzir/concept/printable/numeric/integral.hpp"
#include "tenzir/concept/printable/string/any.hpp"
#include "tenzir/concept/printable/string/string.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/uri.hpp"

#include <string>

namespace tenzir {

struct key_value_printer : printer_base<key_value_printer> {
  using attribute = std::pair<std::string, std::string>;

  template <class Iterator>
  bool
  print(Iterator& out, const std::pair<std::string, std::string>& kv) const {
    using namespace printers;
    return str.print(out, detail::percent_escape(kv.first))
           and str.print(out, "=")
           and str.print(out, detail::percent_escape(kv.second));
  }
};

template <>
struct printer_registry<std::pair<std::string, std::string>> {
  using type = key_value_printer;
};

struct uri_printer : printer_base<uri_printer> {
  using attribute = uri;

  template <class Iterator>
  bool print(Iterator& out, const uri& u) const {
    using namespace printers;

    if (u.scheme != "") {
      if (not(str.print(out, u.scheme) and any.print(out, ':'))) {
        return false;
      }
    }
    if (u.host != "") {
      if (not(str.print(out, "//")
              and str.print(out, detail::percent_escape(u.host)))) {
        return false;
      }
    }
    if (u.port != 0) {
      if (not(any.print(out, ':') and u16.print(out, u.port))) {
        return false;
      }
    }

    if (not(any.print(out, '/'))) {
      return false;
    }
    if (not detail::print_delimited(u.path.begin(), u.path.end(), out, '/')) {
      return false;
    }

    if (not u.query.empty()) {
      if (not(any.print(out, '?'))) {
        return false;
      }

      auto begin = u.query.begin();
      auto end = u.query.end();
      auto p = make_printer<std::pair<std::string, std::string>>();
      if (not(p.print(out, *begin))) {
        return false;
      }
      while (++begin != end) {
        if (not(any.print(out, '&') and p.print(out, *begin))) {
          return false;
        }
      }

      /*if (!detail::print_delimited(u.query.begin(), u.query.end(), out, '&'))
        return false;*/
    }
    if (u.fragment != "") {
      if (not(any.print(out, '#')
              and str.print(out, detail::percent_escape(u.fragment)))) {
        return false;
      }
    }
    return true;
  }
};

template <>
struct printer_registry<uri> {
  using type = uri_printer;
};

} // namespace tenzir
