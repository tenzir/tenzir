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

#include "vast/detail/type_traits.hpp"
#include "vast/logger.hpp"

namespace vast::detail {

namespace {

template <class Policy>
void merge_settings_impl(const caf::settings& src, caf::settings& dst,
                         Policy policy, size_t depth = 0) {
  if (depth > 100) {
    VAST_ERROR_ANON("Exceeded maximum nesting depth in settings.");
    return;
  }
  for (auto& [key, value] : src) {
    if (caf::holds_alternative<caf::settings>(value)) {
      merge_settings_impl(caf::get<caf::settings>(value),
                          dst[key].as_dictionary(), policy, depth + 1);
    } else {
      if constexpr (std::is_same_v<Policy, policy::merge_lists_tag>) {
        if (caf::holds_alternative<caf::config_value::list>(value)) {
          const auto& src_list = caf::get<caf::config_value::list>(value);
          if (caf::holds_alternative<caf::config_value::list>(dst[key])) {
            auto& dst_list = dst[key].as_list();
            dst_list.insert(dst_list.end(), src_list.begin(), src_list.end());
          } else {
            dst.insert_or_assign(key, src_list);
          }
        } else {
          dst.insert_or_assign(key, value);
        }
      } else if constexpr (std::is_same_v<Policy, policy::overwrite_lists_tag>) {
        dst.insert_or_assign(key, value);
      } else {
        static_assert(detail::always_false_v<Policy>, "unsupported merge "
                                                      "policy");
      }
    }
  }
}

} // namespace

void merge_settings(const caf::settings& src, caf::settings& dst,
                    policy::overwrite_lists_tag policy) {
  return merge_settings_impl(src, dst, policy);
}

void merge_settings(const caf::settings& src, caf::settings& dst,
                    policy::merge_lists_tag policy) {
  return merge_settings_impl(src, dst, policy);
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
