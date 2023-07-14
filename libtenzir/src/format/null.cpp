//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/format/null.hpp"

#include "tenzir/fwd.hpp"

namespace tenzir::format::null {

writer::writer(ostream_ptr out, const caf::settings&) : super{std::move(out)} {
}

caf::error writer::write(const table_slice&) {
  return caf::none;
}

const char* writer::name() const {
  return "null-writer";
}

} // namespace tenzir::format::null
