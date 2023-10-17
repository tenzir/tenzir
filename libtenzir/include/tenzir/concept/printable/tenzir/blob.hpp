//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/string/escape.hpp"
#include "tenzir/detail/escapers.hpp"

namespace tenzir {

template <class T>
struct generic_blob_printer : printer_base<generic_blob_printer<T>> {
  using attribute = T;

  template <class Iterator>
  bool print(Iterator& out, const attribute& x) const {
    // TODO: Check that this works as expected.
    static auto escaper = detail::make_extra_print_escaper("\"");
    static auto p = "b\"" << printers::escape(escaper) << '"';
    return p(out, std::string_view{reinterpret_cast<const char*>(x.data()),
                                   x.size()});
  }
};

} // namespace tenzir
