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

#pragma once

#include <string>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"
#include "vast/uri.hpp"
#include "vast/detail/string.hpp"

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

