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

#include <simdjson.h>

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
        [&string_value](const list_type& lt) -> caf::expected<data> {
          if (not caf::holds_alternative<string_type>(lt.value_type())) {
            return caf::make_error(ec::invalid_argument,
                                   "currently only strings in lists are "
                                   "accepted");
          }
          ::simdjson::dom::parser p;
          auto el = p.parse(string_value);
          if (el.error() != ::simdjson::error_code::SUCCESS) {
            return caf::make_error(ec::invalid_argument,
                                   "not a valid JSON value");
          }
          auto obj = el.value().get_array();
          if (obj.error() != ::simdjson::error_code::SUCCESS) {
            return caf::make_error(ec::invalid_argument,
                                   "not a valid JSON array");
          }
          auto l = list{};
          for (const auto& x : obj.value()) {
            if (x.type() != ::simdjson::dom::element_type::STRING) {
              return caf::make_error(ec::invalid_argument,
                                     "currently only string values in arrays "
                                     "are accepted");
            }
            l.emplace_back(std::string{x.get_string().value()});
          }
          return l;
        },
        [&string_value]<basic_type Type>(const Type&) -> caf::expected<data> {
          using data_t = type_to_data_t<Type>;
          auto result = to<data_t>(string_value);
          if (!result)
            return result.error();
          return *result;
        },
        []<complex_type Type>(const Type&) -> caf::expected<data> {
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

rest_response::rest_response(std::string body) : body_(std::move(body)) {
}

auto rest_response::is_error() const -> bool {
  return is_error_;
}

auto rest_response::body() const -> const std::string& {
  return body_;
}

auto rest_response::code() const -> size_t {
  return code_;
}

auto rest_response::error_detail() const -> const caf::error& {
  return detail_;
}

auto rest_response::release() && -> std::string {
  return std::move(body_);
}

auto rest_response::make_error(uint16_t error_code, std::string message,
                               caf::error detail) -> rest_response {
  auto result = rest_response{fmt::format("{{\"error\": {:?}}}\n", message)};
  result.code_ = error_code;
  result.is_error_ = true;
  result.detail_ = std::move(detail);
  return result;
}

} // namespace vast
