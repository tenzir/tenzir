//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/http_api.hpp>

#include <fmt/format.h>

#include <simdjson.h>

namespace {

// Validates that the argument is a valid JSON object.
[[maybe_unused]] auto validate_json(const std::string& json) -> bool {
  auto parser = simdjson::dom::parser{};
  auto padded = simdjson::padded_string{json};
  auto obj = simdjson::dom::object{};
  return parser.parse(json).get(obj) == simdjson::SUCCESS;
}

template <typename T>
auto to_json_oneline(const T& obj) -> std::string {
  auto printer = tenzir::json_printer{{.oneline = true}};
  auto result = std::string{};
  auto out = std::back_inserter(result);
  printer.print(out, obj);
  return result;
}

} // namespace

namespace tenzir {

// TODO: Consider replacing with magic_enum
auto http_method_from_string(const std::string& str)
  -> caf::expected<http_method> {
  static constexpr std::array<std::pair<http_method, const char*>, 6> method_map
    = {{
      {http_method::get, "GET"},
      {http_method::post, "POST"},
      {http_method::put, "PUT"},
      {http_method::head, "HEAD"},
      {http_method::options, "OPTIONS"},
      {http_method::delete_, "DELETE"},
    }};
  constexpr int num_methods = [](http_method m) {
    // Use a switch-statement to force a compiler error if we forget
    // to add a new enum value.
    switch (m) {
      case http_method::get:
      case http_method::post:
      case http_method::put:
      case http_method::head:
      case http_method::options:
      case http_method::delete_:
        return 6;
    }
  }(http_method::get);
  static_assert(method_map.size() == num_methods);
  for (auto const& [method, method_string] : method_map)
    if (str == method_string)
      return method;
  return caf::make_error(ec::invalid_argument, "unknown method");
}

auto rest_endpoint::canonical_path() const -> std::string {
  // eg. "POST /query/:id/next (v0)"
  return fmt::format("{} {} ({})", method, path, version);
}

auto parse_skeleton(simdjson::ondemand::value value, size_t depth = 0)
  -> tenzir::data {
  if (depth > tenzir::defaults::max_recursion)
    return tenzir::data{"nesting too deep"};
  switch (value.type()) {
    case simdjson::ondemand::json_type::null:
      return tenzir::data{};
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
  try {
    http_parameter_map result;
    if (json.empty())
      return result;
    auto padded_string = simdjson::padded_string{json};
    auto parser = simdjson::ondemand::parser{};
    auto doc = parser.iterate(padded_string);
    for (auto obj : doc.get_object()) {
      auto value = parse_skeleton(obj.value());
      // Discard null values
      if (caf::holds_alternative<caf::none_t>(value))
        continue;
      result.params_.emplace(std::string{obj.unescaped_key().value()}, value);
    }
    return result;
  } catch (const simdjson::simdjson_error& exc) {
    return caf::make_error(ec::invalid_argument,
                           fmt::format("failed to parse json: {}", exc.what()));
  }
}

auto http_parameter_map::params() const
  -> const detail::stable_map<std::string, tenzir::data>& {
  return params_;
}

auto http_parameter_map::get_unsafe()
  -> detail::stable_map<std::string, tenzir::data>& {
  return params_;
}

auto http_parameter_map::emplace(std::string&& key, tenzir::data&& value)
  -> void {
  // TODO: Validate that `value` contains only strings, lists and records.
  params_.emplace(std::move(key), std::move(value));
}

// Note that we currently only supports basic types and lists of basic
// types as endpoint parameters. Ultimately, we probably want to switch
// to a recursive algorithm here to allow endpoints to declare arbitrarily
// complex types as input.
auto parse_endpoint_parameters(const tenzir::rest_endpoint& endpoint,
                               const http_parameter_map& parameter_map)
  -> caf::expected<tenzir::record> {
  tenzir::record result;
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
        [&](const blob_type&) -> caf::expected<data> {
          return caf::make_error(ec::invalid_argument,
                                 "blob parameters are not supported");
        },
        [&](const bool_type&) -> caf::expected<data> {
          if (!is_string)
            return caf::make_error(ec::invalid_argument, "expected bool");
          bool result = false;
          auto const& string_value = as<std::string>(param_data);
          if (!parsers::boolean(string_value, result))
            return caf::make_error(ec::invalid_argument, "not a boolean value");
          return data{result};
        },
        [&](const list_type& lt) -> caf::expected<data> {
          auto const* list = caf::get_if<tenzir::list>(&param_data);
          if (!list)
            return caf::make_error(ec::invalid_argument, "expected a list");
          auto result = tenzir::list{};
          for (auto const& x : *list) {
            if (!caf::holds_alternative<std::string>(x)
                and !caf::holds_alternative<tenzir::record>(x))
              return caf::make_error(ec::invalid_argument,
                                     "expected a string or record");
            auto const& x_as_string = as<std::string>(x);
            auto parsed = caf::visit(
              detail::overload{
                [&](const string_type&) -> caf::expected<data> {
                  return x_as_string;
                },
                [&](const blob_type&) -> caf::expected<data> {
                  return caf::make_error(ec::invalid_argument,
                                         "blob parameters are not supported");
                },
                [&]<basic_type Type>(const Type&) -> caf::expected<data> {
                  using data_t = type_to_data_t<Type>;
                  auto result = to<data_t>(x_as_string);
                  if (!result)
                    return std::move(result.error());
                  return std::move(*result);
                },
                [&](const record_type&) -> caf::expected<data> {
                  // TODO: This currently works with records containing only
                  // strings, but is untested with other value types.
                  const auto* result = caf::get_if<record>(&x);
                  if (!result)
                    return caf::make_error(ec::invalid_argument,
                                           fmt::format("invalid record "
                                                       "syntax"));
                  return *result;
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
        [&](const record_type&) -> caf::expected<data> {
          const auto* parsed = caf::get_if<record>(&param_data);
          if (!parsed)
            return caf::make_error(ec::invalid_argument, "expected a record");
          auto result = record{};
          for (const auto& x : *parsed) {
            auto parse_result = false;
            const auto* str = caf::get_if<std::string>(&x.second);
            if (not str) {
              return caf::make_error(ec::invalid_argument,
                                     "currently only non-null boolean values "
                                     "in nested records "
                                     "are supported");
            }
            if (!parsers::boolean(*str, parse_result)) {
              return caf::make_error(ec::invalid_argument,
                                     "currently only non-null boolean values "
                                     "in nested records "
                                     "are supported");
            }
            result[x.first] = parse_result;
          }
          return result;
        },
        [&]<basic_type Type>(const Type&) -> caf::expected<data> {
          using data_t = type_to_data_t<Type>;
          if (!is_string)
            return caf::make_error(ec::invalid_argument, "expected basic type");
          auto const& string_value = as<std::string>(param_data);
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

rest_response::rest_response(const tenzir::record& data)
  : body_(to_json_oneline(data)) {
}

auto rest_response::from_json_string(std::string json) -> rest_response {
  TENZIR_ASSERT_EXPENSIVE(validate_json(json));
  auto result = rest_response{};
  result.code_ = 200;
  result.body_ = std::move(json);
  return result;
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

auto rest_response::make_error(uint16_t error_code, std::string_view message,
                               caf::error detail) -> rest_response {
  return make_error_raw(error_code,
                        fmt::format("{{\"error\": {}}}\n",
                                    detail::json_escape(message)),
                        std::move(detail));
}

auto rest_response::make_error_raw(uint16_t error_code, std::string body,
                                   caf::error detail) -> rest_response {
  auto result = rest_response{};
  TENZIR_ASSERT_EXPENSIVE(validate_json(body));
  result.body_ = std::move(body);
  result.code_ = error_code;
  result.is_error_ = true;
  result.detail_ = std::move(detail);
  return result;
}

} // namespace tenzir
