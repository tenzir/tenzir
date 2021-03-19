//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/settings.hpp>

namespace vast::policy {

struct merge_lists_tag {};
struct overwrite_lists_tag {};

inline static constexpr merge_lists_tag merge_lists{};
inline static constexpr overwrite_lists_tag overwrite_lists{};

} // namespace vast::policy

namespace vast::detail {

/// Merge settings of `src` into `dst`, overwriting existing values from `dst`
/// if necessary. Passing `policy::merge_lists` enables merging of nested lists.
void merge_settings(const caf::settings& src, caf::settings& dst,
                    policy::overwrite_lists_tag policy
                    = policy::overwrite_lists);

/// Merge settings of `src` into `dst`, overwriting existing values from `dst`
/// if necessary. Passing `policy::merge_lists` enables merging of nested lists.
void merge_settings(const caf::settings& src, caf::settings& dst,
                    policy::merge_lists_tag policy);

/// Remove empty settings objects from the tree.
/// Example:
///   { a = 13, b = {}, c = { d = {} } }
/// is changed into:
///   { a = 13 }
bool strip_settings(caf::settings& xs);

/// Returns the amount of bytes specified by a config option.
/// * If the key has a string or integer key giving a size, use that.
/// * If the key doesn't exist, use the provided default value.
/// * If the key exists with a different type, return an error.
/// * If the key exists but cant be parsed as a byte size, return an error.
caf::expected<uint64_t>
get_bytesize(caf::settings opts, std::string_view key, uint64_t defval);

} // namespace vast::detail
