//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/access.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/pattern.hpp"

namespace vast {

template <>
struct access::printer<vast::pattern>
  : printer_base<access::printer<vast::pattern>> {
  using attribute = pattern;

  template <class Iterator>
  bool print(Iterator& out, const pattern& pat) const {
    auto p = '/' << printers::str << ((pat.case_insensitive_) ? "/i" : "/");
    return p.print(out, pat.str_);
  }
};

template <>
struct printer_registry<pattern> {
  using type = access::printer<pattern>;
};

} // namespace vast
