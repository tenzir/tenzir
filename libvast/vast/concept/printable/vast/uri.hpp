//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/detail/string.hpp"
#include "vast/uri.hpp"

#include <string>

namespace vast {

struct key_value_printer : printer<key_value_printer> {
  using attribute = std::pair<std::string,std::string>;

  template <class Iterator>
  bool print(Iterator& out, const std::pair<std::string,std::string>& kv) const {
    using namespace printers;
    return str.print(out, detail::percent_escape(kv.first)) 
        && str.print(out, "=") 
        && str.print(out, detail::percent_escape(kv.second));
  }
};

template <>
struct printer_registry<std::pair<std::string,std::string>> {
  using type = key_value_printer;
};

struct uri_printer : printer<uri_printer> {
  using attribute = uri;
  
  template <class Iterator>
  bool print(Iterator& out, const uri& u) const {
    using namespace printers;
    
    if (u.scheme != "") {
      if (!(str.print(out, u.scheme) && any.print(out, ':')))
        return false;
    }
    if (u.host != "") {
      if (!(str.print(out, "//") && str.print(out, detail::percent_escape(u.host))))
        return false;
    }
    if (u.port != 0) {
      if (!(any.print(out, ':') && u16.print(out, u.port)))
        return false;
    }
    
    if (!(any.print(out, '/')))
      return false;
    if (!detail::print_delimited(u.path.begin(), u.path.end(), out, '/'))
      return false;
    
    if (!u.query.empty()) {
      if (!(any.print(out, '?')))
        return false;
      
      auto begin = u.query.begin();
      auto end = u.query.end();
      auto p = make_printer<std::pair<std::string,std::string>>();
      if (!(p.print(out,*begin)))
          return false;
      while (++begin != end)
        if (!(any.print(out, '&') && p.print(out,*begin)))
          return false;
      
      /*if (!detail::print_delimited(u.query.begin(), u.query.end(), out, '&'))
        return false;*/
    }
    if (u.fragment != "") {
      if (!(any.print(out, '#') && str.print(out, detail::percent_escape(u.fragment))))
        return false;
    }
    return true;
  }
};

template <>
struct printer_registry<uri> {
  using type = uri_printer;
};

} // namespace vast

