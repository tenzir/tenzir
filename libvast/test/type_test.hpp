//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/offset.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"

#include <cstddef>
#include <vector>

/// Returns the type at `offset{xs...}`.
template <class... Offsets>
const vast::type& at(const vast::legacy_record_type& rec, Offsets... xs) {
  auto ptr = rec.at(vast::offset{static_cast<size_t>(xs)...});
  if (!ptr)
    FAIL("offset lookup failed at " << std::vector<int>{xs...});
  return ptr->type;
}

/// Returns the record type at `offset{xs...}`.
template <class... Offsets>
const vast::legacy_record_type&
rec_at(const vast::legacy_record_type& rec, Offsets... xs) {
  auto& t = at(rec, xs...);
  if (!caf::holds_alternative<vast::legacy_record_type>(t))
    FAIL("expected a record type at offset " << std::vector<int>{xs...});
  return caf::get<vast::legacy_record_type>(t);
}
