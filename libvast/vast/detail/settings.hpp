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

struct deep_tag {};
struct shallow_tag {};

inline static constexpr deep_tag deep{};
inline static constexpr shallow_tag shallow{};

} // namespace vast::policy

namespace vast::detail {

/// Merge settings of `src` into `dst`, overwriting existing values from `dst`
/// if necessary. Passing `policy::deep` enables merging of nested arrays.
void merge_settings(const caf::settings& src, caf::settings& dst,
                    policy::shallow_tag policy = policy::shallow);

/// Merge settings of `src` into `dst`, overwriting existing values from `dst`
/// if necessary. Passing `policy::deep` enables merging of nested arrays.
void merge_settings(const caf::settings& src, caf::settings& dst,
                    policy::deep_tag policy);

/// Remove empty settings objects from the tree.
/// Example:
///   { a = 13, b = {}, c = { d = {} } }
/// is changed into:
///   { a = 13 }
bool strip_settings(caf::settings& xs);

} // namespace vast::detail
