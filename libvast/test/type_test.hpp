// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstddef>
#include <vector>

#include "vast/offset.hpp"
#include "vast/type.hpp"

#include "vast/test/test.hpp"

/// Returns the type at `offset{xs...}`.
template <class... Offsets>
const vast::type& at(const vast::record_type& rec, Offsets... xs) {
  auto ptr = rec.at(vast::offset{static_cast<size_t>(xs)...});
  if (!ptr)
    FAIL("offset lookup failed at " << std::vector<int>{xs...});
  return ptr->type;
}

/// Returns the record type at `offset{xs...}`.
template <class... Offsets>
const vast::record_type& rec_at(const vast::record_type& rec, Offsets... xs) {
  auto& t = at(rec, xs...);
  if (!caf::holds_alternative<vast::record_type>(t))
    FAIL("expected a record type at offset " << std::vector<int>{xs...});
  return caf::get<vast::record_type>(t);
}
