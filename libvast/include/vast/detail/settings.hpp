//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/policy/merge_lists.hpp"

#include <caf/settings.hpp>

namespace vast::detail {

/// Merge settings of `src` into `dst`, overwriting existing values from `dst`
/// if necessary.
void merge_settings(const caf::settings& src, caf::settings& dst,
                    enum policy::merge_lists merge_lists);

/// Returns the amount of bytes specified by a config option.
/// * If the key has a string or integer key giving a size, use that.
/// * If the key doesn't exist, use the provided default value.
/// * If the key exists with a different type, return an error.
/// * If the key exists but cant be parsed as a byte size, return an error.
caf::expected<uint64_t>
get_bytesize(caf::settings opts, std::string_view key, uint64_t defval);

} // namespace vast::detail
