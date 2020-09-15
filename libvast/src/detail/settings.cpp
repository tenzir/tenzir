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

#include "vast/detail/settings.hpp"

#include "vast/logger.hpp"

namespace vast::detail {

namespace {

void merge_settings_impl(const caf::settings& src, caf::settings& dst,
                         size_t depth = 0) {
  if (depth > 100) {
    VAST_ERROR_ANON("Exceeded maximum nesting depth in settings.");
    return;
  }
  for (auto& [key, value] : src) {
    if (caf::holds_alternative<caf::settings>(value))
      merge_settings_impl(caf::get<caf::settings>(value),
                          dst[key].as_dictionary(), depth + 1);
    else
      dst.insert_or_assign(key, value);
  }
}

} // namespace

/// Merge settings of `src` into `dst`, overwriting existing values
/// from `dst` if necessary.
void merge_settings(const caf::settings& src, caf::settings& dst) {
  return merge_settings_impl(src, dst);
}

bool strip_settings(caf::settings& xs) {
  auto& m = xs.container();
  for (auto it = m.begin(); it != m.end();) {
    if (auto x = caf::get_if<caf::settings>(&it->second)) {
      if (x->empty()) {
        it = m.erase(it);
      } else {
        if (strip_settings(*x))
          it = m.erase(it);
        else
          ++it;
      }
    } else
      ++it;
  }
  return m.empty();
}

} // namespace vast::detail
