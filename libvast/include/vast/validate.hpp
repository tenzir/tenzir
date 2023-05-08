//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/data.hpp>
#include <vast/type.hpp>

#include <caf/error.hpp>

namespace vast {

enum class validate {
  // No data must have an incompatible schema entry
  // and all required fields exist.
  // Ensures forward compatibility by skipping over unknown fields.
  permissive,
  // All data must have a compatible schema entry and all required
  // fields exist.
  strict,
  // All fields are treated as required. Mostly useful for tests.
  exhaustive,
};

/// A convenience type for using the `opaque` attribute defined below.
static inline const auto opaque_record
  = type{record_type{
           {"dummy", string_type{}}, // Record types may not be empty
         },
         {{"opaque"}}};

/// Check that all keys in `data` are found in `configuration::schema` with
/// the correct type.
/// The `validate` behavior can be adjusted using type attributes:
///    - required: This field must always be present.
///    - opaque: (only on records) Don't validate the contents of this record.
caf::error validate(const vast::data&, const vast::record_type& schema,
                    enum validate mode);

} // namespace vast
