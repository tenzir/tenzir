//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/dissector.hpp"

#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"

namespace tenzir {

namespace {

using namespace parser_literals;

auto make_data_parser() {
  auto str = +(parsers::printable - '}');
  // clang-format off
  auto p
    = parsers::time
    | parsers::duration
    | parsers::net
    | parsers::ip
    | parsers::number
    | parsers::boolean
    | str;
  // clang-format on
  return p;
}

auto make_dissect_parser() {
  auto make_literal = [](std::string str) {
    // FIXME
    fmt::print("literal: '{}'\n", str);
    return dissector::token{dissector::literal{
      .parser = parsers::str{std::move(str)},
    }};
  };
  auto make_field = [](std::string str) {
    // The skip field notation is %{?foo} or %{}. Skipped fields are equivalent
    // to literals, i.e., we parse them but don't add them to the output.
    auto skip = false;
    if (str.empty()) {
      skip = true;
    } else if (str.starts_with("?")) {
      skip = true;
      str.erase(str.begin());
    }
    // FIXME
    fmt::print("field: '{}'\n", str);
    return dissector::token{dissector::field{
      .name = std::move(str),
      .skip = skip,
      .parser = make_data_parser(),
    }};
  };
  auto field_char = parsers::printable - '}';
  auto field = "%{"_p >> *field_char >> '}';
  auto skip_char = parsers::printable - '%';
  auto skip = +skip_char;
  // clang-format off
  auto section
    = field ->* make_field
    | skip ->* make_literal
    ;
  // clang-format on
  auto parser = +section;
  return parser;
}

} // namespace

auto dissector::make(std::string_view pattern, dissector_style style)
  -> caf::expected<dissector> {
  auto result = dissector{};
  switch (style) {
    case dissector_style::grok:
      return caf::make_error(ec::unimplemented);
    case dissector_style::dissect: {
      static auto parser = make_dissect_parser();
      if (not parser(pattern, result.tokens_))
        // TODO: use diagnostics
        return caf::make_error(ec::parse_error);
      break;
    }
    case dissector_style::kv:
      return caf::make_error(ec::unimplemented);
  }
  return result;
}

auto dissector::dissect(std::string_view input) -> std::optional<record> {
  auto result = record{};
  const auto* begin = input.begin();
  const auto* end = input.end();
  for (const auto& section : tokens_) {
    auto f = detail::overload{
      [&](const field& field) -> std::optional<diagnostic> {
        auto x = data{};
        // FIXME
        fmt::print("[{}, {}): {} = '{}'\n", begin - input.begin(),
                   end - input.begin(), field.name, std::string{begin, end});
        if (field.parser(begin, end, x)) {
          if (not field.skip)
            result.emplace(field.name, std::move(x));
        } else if (begin == end) {
          if (not field.skip)
            result.emplace(field.name, data{});
        } else {
          return diagnostic::error("failed to dissect field")
            .note("field: {}", field.name)
            .done();
        }
        return std::nullopt;
      },
      [&](const literal& literal) -> std::optional<diagnostic> {
        // FIXME
        fmt::print("[{}, {}): literal with '{}'\n", begin - input.begin(),
                   end - input.begin(), std::string{begin, end});
        if (not literal.parser(begin, end))
          return diagnostic::error("failed to dissect literal").done();
        return std::nullopt;
      },
    };
    if (auto diag = std::visit(f, section)) {
      TENZIR_ERROR(diag);
      return std::nullopt;
    }
  }
  return result;
}

auto dissector::tokens() -> const std::vector<token>& {
  return tokens_;
}

} // namespace tenzir
