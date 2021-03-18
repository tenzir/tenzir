// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/null.hpp"

#include "vast/fwd.hpp"

namespace vast::format::null {

writer::writer(ostream_ptr out, const caf::settings&) : super{std::move(out)} {
}

caf::error writer::write(const table_slice&) {
  return caf::none;
}

const char* writer::name() const {
  return "null-writer";
}

} // namespace vast::format::null
