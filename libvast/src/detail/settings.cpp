//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/settings.hpp"

#include "vast/concept/parseable/vast/si.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/logger.hpp"

#include <caf/expected.hpp>

namespace vast::detail {

namespace {

void merge_settings_impl(const caf::settings& src, caf::settings& dst,
                         enum policy::merge_lists merge_lists, size_t depth) {
  if (depth > 100) {
    VAST_ERROR("Exceeded maximum nesting depth in settings.");
    return;
  }
  for (auto& [key, value] : src) {
    if (caf::holds_alternative<caf::settings>(value)) {
      merge_settings_impl(caf::get<caf::settings>(value),
                          dst[key].as_dictionary(), merge_lists, depth + 1);
    } else {
      if (merge_lists == policy::merge_lists::yes) {
        if (caf::holds_alternative<caf::config_value::list>(value)
            && caf::holds_alternative<caf::config_value::list>(dst[key])) {
          const auto& src_list = caf::get<caf::config_value::list>(value);
          auto& dst_list = dst[key].as_list();
          dst_list.insert(dst_list.end(), src_list.begin(), src_list.end());
        } else {
          dst.insert_or_assign(key, value);
        }
      } else if (merge_lists == policy::merge_lists::no) {
        VAST_ASSERT(merge_lists == policy::merge_lists::no, //
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

std::string
convert_to_caf_compatible_list_arg(const std::string& comma_separated_list_arg) {
  constexpr auto arg_value_seperator = std::string_view{"="};
  const auto separation_index
    = comma_separated_list_arg.find_first_of(arg_value_seperator);
  const auto begin = comma_separated_list_arg.begin();
  if (separation_index == std::string::npos)
    return {};
  const auto arg_start_it
    = begin + separation_index + arg_value_seperator.length();
  if (arg_start_it == comma_separated_list_arg.end())
    return comma_separated_list_arg;
  const auto arg_name = std::string_view{begin, begin + separation_index};
  const auto arg
    = std::string_view(arg_start_it, comma_separated_list_arg.end());
  const auto split_args = detail::split(arg, ",", "\\");
  return fmt::format("{}=[\"{}\"]", arg_name, fmt::join(split_args, "\",\""));
}

} // namespace vast::detail
