//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/settings.hpp"

#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/logger.hpp"

#include <caf/expected.hpp>

namespace tenzir::detail {

namespace {

void merge_settings_impl(const caf::settings& src, caf::settings& dst,
                         enum policy::merge_lists merge_lists, size_t depth) {
  if (depth > 100) {
    TENZIR_ERROR("Exceeded maximum nesting depth in settings.");
    return;
  }
  for (auto& [key, value] : src) {
    if (caf::holds_alternative<caf::settings>(value)) {
      merge_settings_impl(as<caf::settings>(value), dst[key].as_dictionary(),
                          merge_lists, depth + 1);
    } else {
      if (merge_lists == policy::merge_lists::yes) {
        if (caf::holds_alternative<caf::config_value::list>(value)
            && caf::holds_alternative<caf::config_value::list>(dst[key])) {
          const auto& src_list = as<caf::config_value::list>(value);
          auto& dst_list = dst[key].as_list();
          dst_list.insert(dst_list.end(), src_list.begin(), src_list.end());
        } else {
          dst.insert_or_assign(key, value);
        }
      } else if (merge_lists == policy::merge_lists::no) {
        TENZIR_ASSERT(merge_lists == policy::merge_lists::no, //
                      "unsupported merge policy");
        dst.insert_or_assign(key, value);
      }
    }
  }
}

} // namespace

void merge_settings(const caf::settings& src, caf::settings& dst,
                    enum policy::merge_lists merge_lists) {
  merge_settings_impl(src, dst, merge_lists, 0);
}

caf::expected<uint64_t>
get_bytesize(caf::settings opts, std::string_view key, uint64_t defval) {
  // Note that there's no `caf::has_key()` and e.g. `caf::get_or<std::string>`
  // would silently take the default value if the key exists but is not a
  // string, so we have to make a copy of `opts` and use `caf::put_missing()`
  // as a workaround.
  size_t result = 0;
  caf::put_missing(opts, key, defval);
  if (caf::holds_alternative<caf::config_value::integer>(opts, key)) {
    result = caf::get<caf::config_value::integer>(opts, key);
  } else if (caf::holds_alternative<std::string>(opts, key)) {
    auto result_str = caf::get<std::string>(opts, key);
    if (!parsers::bytesize(result_str, result))
      return caf::make_error(ec::parse_error, "could not parse '" + result_str
                                                + "' as valid byte size");
  } else {
    return caf::make_error(ec::invalid_argument,
                           "invalid value for key '" + std::string{key} + "'");
  }
  return result;
}

} // namespace tenzir::detail
