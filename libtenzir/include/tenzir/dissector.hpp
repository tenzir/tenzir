//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/rule.hpp"
#include "tenzir/data.hpp"

#include <caf/expected.hpp>

#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace tenzir {

enum class dissector_style {
  grok,    ///< A Logstash grok pattern.
  dissect, ///< Similar to Elastic's `dissect` plugin.
  kv,      ///< A list of key-value pairs.
};

/// Dissects strings according to a given set of rules.
class dissector {
public:
  /// A section that will end up in the output record.
  struct field {
    std::string name;
    bool skip;
    rule<std::string_view::iterator, data> parser;
  };

  /// A section that will be parsed but dropped.
  struct literal {
    rule<std::string_view::iterator, unused_type> parser;
  };

  /// The sum type describing possible types of sections.
  using token = std::variant<literal, field>;

  static auto make(std::string_view pattern, dissector_style style = {})
    -> caf::expected<dissector>;

  /// Parses a string into a record.
  /// @param input The input to parse.
  /// @returns The parsed record.
  auto dissect(std::string_view input) -> std::optional<record>;

  /// Retrieves the list of tokens.
  auto tokens() -> const std::vector<token>&;

private:
  dissector() = default;

  std::vector<token> tokens_;
};

} // namespace tenzir
