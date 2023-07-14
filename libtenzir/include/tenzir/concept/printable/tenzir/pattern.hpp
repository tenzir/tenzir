//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/access.hpp"
#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/string/any.hpp"
#include "tenzir/concept/printable/string/escape.hpp"
#include "tenzir/concept/printable/string/string.hpp"
#include "tenzir/detail/escapers.hpp"
#include "tenzir/pattern.hpp"

namespace tenzir {

template <>
struct access::printer<tenzir::pattern>
  : printer_base<access::printer<tenzir::pattern>> {
  using attribute = pattern;

  template <class Iterator>
  bool print(Iterator& out, const pattern& pat) const {
    auto escaper = detail::make_extra_print_escaper("/");
    auto p = '/' << printers::escape(escaper)
                 << ((pat.options_.case_insensitive) ? "/i" : "/");
    return p.print(out, pat.str_);
  }
};

template <>
struct printer_registry<pattern> {
  using type = access::printer<pattern>;
};

} // namespace tenzir
