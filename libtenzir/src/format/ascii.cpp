//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/format/ascii.hpp"

#include "tenzir/concept/printable/tenzir/view.hpp"
#include "tenzir/policy/flatten_schema.hpp"
#include "tenzir/table_slice.hpp"

namespace tenzir::format::ascii {

writer::writer(ostream_ptr out, const caf::settings&) : super{std::move(out)} {
  // nop
}

caf::error writer::write(const table_slice& x) {
  data_view_printer printer;
  return print<policy::flatten_schema>(printer, x, {", ", ": ", "<", ">"});
}

const char* writer::name() const {
  return "ascii-writer";
}

} // namespace tenzir::format::ascii
