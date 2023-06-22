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

auto parse_skeleton(simdjson::ondemand::value value, size_t depth = 0)
  -> vast::data {
  if (depth > vast::defaults::max_recursion)
    return vast::data{"nesting too deep"};
  switch (value.type()) {
    case simdjson::ondemand::json_type::null:
      return vast::data{};
    case simdjson::ondemand::json_type::number:
    case simdjson::ondemand::json_type::boolean:
      return std::string{value.raw_json_token()};
    case simdjson::ondemand::json_type::string:
      return std::string{value.get_string().value()};
    case simdjson::ondemand::json_type::array: {
      auto lst = value.get_array();
      list xs;
      for (auto x : lst)
        xs.push_back(parse_skeleton(x.value(), depth + 1));
      return xs;
    }
    case simdjson::ondemand::json_type::object: {
      record xs;
      auto obj = value.get_object();
      for (auto pair : obj)
        xs.emplace(std::string{pair.unescaped_key().value()},
                   parse_skeleton(pair.value().value(), depth + 1));
      return xs;
    }
  }
  die("missing return in switch statement");
}

auto http_parameter_map::from_json(std::string_view json)
  -> caf::expected<http_parameter_map> {
  http_parameter_map result;
  if (json.empty())
    return result;
  auto padded_string = simdjson::padded_string{json};
  auto parser = simdjson::ondemand::parser{};
  auto doc = simdjson::ondemand::document{};
  auto body = simdjson::ondemand::object{};
  if ([[maybe_unused]] auto error = parser.iterate(padded_string).get(doc)) {
    std::stringstream ss;
    ss << error;
    auto error_msg = fmt::format("failed to parse json: {}", ss.str());
    return caf::make_error(ec::invalid_argument, error_msg);
  }
  if ([[maybe_unused]] auto error = doc.get_object().get(body))
    return caf::make_error(ec::invalid_argument, "expected a top-level object");
  for (auto obj : body) {
    auto value = parse_skeleton(obj.value());
    // Discard null values
    if (caf::holds_alternative<caf::none_t>(value))
      continue;
    result.params_.emplace(std::string{obj.unescaped_key().value()}, value);
  }
  return result;
}

auto http_parameter_map::params() const
  -> const detail::stable_map<std::string, vast::data>& {
  return params_;
}

auto http_parameter_map::get_unsafe()
  -> detail::stable_map<std::string, vast::data>& {
  return params_;
}

auto http_parameter_map::emplace(std::string&& key, vast::data&& value)
  -> void {
  // TODO: Validate that `value` contains only strings, lists and records.
  params_.emplace(std::move(key), std::move(value));
}

// Note that we currently only supports basic types and lists of basic
// types as endpoint parameters. Ultimately, we probably want to switch
// to a recursive algorithm here to allow endpoints to declare arbitrarily
// complex types as input.
auto parse_endpoint_parameters(const vast::rest_endpoint& endpoint,
                               const http_parameter_map& parameter_map)
  -> caf::expected<vast::record> {
  vast::record result;
  if (!endpoint.params)
    return result;
  auto const& params = parameter_map.params();
  for (auto const& field : endpoint.params->fields()) {
    auto const& name = field.name;
    auto maybe_param = params.find(name);
    if (maybe_param == params.end())
      continue;
    auto const& param_data = maybe_param->second;
    auto is_string = caf::holds_alternative<std::string>(param_data);
    auto typed_value = caf::visit(
      detail::overload{
        [&](const string_type&) -> caf::expected<data> {
          if (!is_string)
            return caf::make_error(ec::invalid_argument, "expected string");
          return param_data;
        },
        [&](const bool_type&) -> caf::expected<data> {
          if (!is_string)
            return caf::make_error(ec::invalid_argument, "expected bool");
          bool result = false;
          auto const& string_value = caf::get<std::string>(param_data);
          if (!parsers::boolean(string_value, result))
            return caf::make_error(ec::invalid_argument, "not a boolean value");
          return data{result};
        },
        [&](const list_type& lt) -> caf::expected<data> {
          auto const* list = caf::get_if<vast::list>(&param_data);
          if (!list)
            return caf::make_error(ec::invalid_argument, "expected a list");
          auto result = vast::list{};
          for (auto const& x : *list) {
            if (!caf::holds_alternative<std::string>(x))
              return caf::make_error(ec::invalid_argument, "expected a string");
            auto const& x_as_string = caf::get<std::string>(x);
            auto parsed = caf::visit(
              detail::overload{
                [&](const string_type&) -> caf::expected<data> {
                  return x_as_string;
                },
                [&]<basic_type Type>(const Type&) -> caf::expected<data> {
                  using data_t = type_to_data_t<Type>;
                  auto result = to<data_t>(x_as_string);
                  if (!result)
                    return std::move(result.error());
                  return std::move(*result);
                },
                [](const auto&) -> caf::expected<data> {
                  return caf::make_error(ec::invalid_argument,
                                         "only lists of basic types are "
                                         "supported");
                },
              },
              lt.value_type());
            if (!parsed)
              return parsed.error();
            result.emplace_back(*parsed);
          }
          return result;
        },
        [&]<basic_type Type>(const Type&) -> caf::expected<data> {
          using data_t = type_to_data_t<Type>;
          if (!is_string)
            return caf::make_error(ec::invalid_argument, "expected basic type");
          auto const& string_value = caf::get<std::string>(param_data);
          auto result = to<data_t>(string_value);
          if (!result)
            return std::move(result.error());
          return std::move(*result);
        },
        []<complex_type Type>(const Type&) -> caf::expected<data> {
          return caf::make_error(ec::invalid_argument,
                                 "REST API only accepts basic type "
                                 "parameters");
        },
      },
      field.type);
    if (!typed_value)
      return caf::make_error(ec::invalid_argument,
                             fmt::format("failed to parse parameter "
                                         "'{}' with value '{}': {}\n",
                                         name, param_data,
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
  return make_error_raw(error_code,
                        fmt::format("{{\"error\": {:?}}}\n", message),
                        std::move(detail));
}

auto rest_response::make_error_raw(uint16_t error_code, std::string body,
                                   caf::error detail) -> rest_response {
  auto result = rest_response{std::move(body)};
  result.code_ = error_code;
  result.is_error_ = true;
  result.detail_ = std::move(detail);
  return result;
}

} // namespace vast
