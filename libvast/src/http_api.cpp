//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/http_api.hpp>

#include <fmt/format.h>

namespace vast {

auto rest_endpoint::canonical_path() const -> std::string {
  // eg. "POST /query/:id/next (v0)"
  return fmt::format("{} {} ({})", method, path, version);
}

auto parse_endpoint_parameters(
  const vast::rest_endpoint& endpoint,
  const detail::stable_map<std::string, std::string>& params)
  -> caf::expected<vast::record> {
  vast::record result;
  if (!endpoint.params)
    return result;
  for (auto const& leaf : endpoint.params->leaves()) {
    auto const& name = leaf.field.name;
    auto maybe_param = params.find(name);
    if (maybe_param == params.end())
      continue;
    auto const& string_value = maybe_param->second;
    auto typed_value = caf::visit(
      detail::overload{
        [&string_value](const string_type&) -> caf::expected<data> {
          return data{string_value};
        },
        [&string_value](const bool_type&) -> caf::expected<data> {
          bool result = false;
          if (!parsers::boolean(string_value, result))
            return caf::make_error(ec::invalid_argument, "not a boolean value");
          return data{result};
        },
        [&string_value]<basic_type Type>(const Type&) -> caf::expected<data> {
          using data_t = type_to_data_t<Type>;
          auto result = to<data_t>(string_value);
          if (!result)
            return result.error();
          return *result;
        },
        []<complex_type Type>(const Type&) -> caf::expected<data> {
          // TODO: Also allow lists.
          return caf::make_error(ec::invalid_argument,
                                 "REST API only accepts basic type "
                                 "parameters");
        },
      },
      leaf.field.type);
    if (!typed_value)
      return caf::make_error(ec::invalid_argument,
                             fmt::format("failed to parse parameter "
                                         "'{}' with value '{}': {}\n",
                                         name, string_value,
                                         typed_value.error()));
    result[name] = std::move(*typed_value);
  }
  return result;
}

} // namespace vast
