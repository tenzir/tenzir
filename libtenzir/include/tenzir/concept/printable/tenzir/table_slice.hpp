//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/numeric/integral.hpp"
#include "tenzir/concept/printable/string/char.hpp"
#include "tenzir/table_slice.hpp"

namespace tenzir {

/// Prints a table slice as ID interval.
struct table_slice_printer : printer_base<table_slice_printer> {
  using attribute = table_slice;

  template <class Iterator>
  bool print(Iterator& out, const table_slice& x) const {
    using namespace printers;
    auto p = '[' << u64 << ',' << u64 << ')';
    return p(out, x.offset(), x.offset() + x.rows());
  }
};

template <>
struct printer_registry<table_slice> {
  using type = table_slice_printer;
};

} // namespace tenzir
