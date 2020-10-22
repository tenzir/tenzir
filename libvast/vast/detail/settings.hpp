/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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

} // namespace vast::detail
