//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/attribute.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string.hpp"

namespace vast {

using namespace std::string_literals;

struct attribute_printer : printer_base<attribute_printer> {
  using attribute = vast::attribute;

  template <class Iterator>
  bool print(Iterator& out, const vast::attribute& attr) const {
    // clang-format off
    using namespace printers;
    using namespace printer_literals;
    auto prepend_eq = [](const std::string& x) { return '=' + x; };
    auto p = '#'_P << str << -(str ->* prepend_eq);
    return p(out, attr.key, attr.value);
    // clang-format on
  }
};

template <>
struct printer_registry<attribute> {
  using type = attribute_printer;
};

} // namespace vast
