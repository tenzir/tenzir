//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/tql/expression.hpp"
#include "vast/tql/parser_interface.hpp"

namespace vast::tql {

/// An argument parser for TQL.
///
/// Supported signatures for `parser.add(...)`:
/// - `foo <meta>`: `add(req, "<meta>")`
/// - `foo [<meta>]`: add(opt, "<meta>")`
/// - `foo [-b|--bar <meta>]`: `add("-b,--bar", xyz, "<meta>")`
/// - `foo [-q|--qux]`: `add("-q,--qux", src)`
class argument_parser {
public:
  explicit argument_parser(std::string name) : name_{std::move(name)} {
  }

  argument_parser(std::string name, std::string docs)
    : name_{std::move(name)}, docs_{std::move(docs)} {
  }

  ~argument_parser() {
    // TODO: What if exception is thrown?
    VAST_ASSERT(called_parse_);
  }

  argument_parser(const argument_parser&) = delete;
  argument_parser(argument_parser&&) = delete;
  auto operator=(const argument_parser&) -> argument_parser& = delete;
  auto operator=(argument_parser&&) -> argument_parser& = delete;

  template <class T>
  void add(T& x, std::string meta) {
    VAST_ASSERT(!first_optional_);
    positional_.push_back(positional_t{
      std::move(meta),
      [&x](located<T> y) {
        x = std::move(y.inner);
      },
    });
  }

  template <class T>
  void add(located<T>& x, std::string meta) {
    VAST_ASSERT(!first_optional_);
    positional_.push_back(positional_t{
      std::move(meta),
      [&x](located<T> y) {
        x = std::move(y);
      },
    });
  }

  template <class T>
  void add(std::optional<T>& x, std::string meta) {
    if (!first_optional_) {
      first_optional_ = positional_.size();
    }
    positional_.push_back(positional_t{
      std::move(meta),
      [&x](located<T> y) {
        x = std::move(y.inner);
      },
    });
  }

  template <class T>
  void add(std::optional<located<T>>& x, std::string meta) {
    if (!first_optional_) {
      first_optional_ = positional_.size();
    }
    positional_.push_back(positional_t{
      std::move(meta),
      [&x](located<T> y) {
        x = std::move(y);
      },
    });
  }

  template <class T>
  void add(std::string_view names, std::optional<T>& x, std::string meta) {
    options_.push_back(option_t{
      split_names(names),
      std::move(meta),
      [&x](located<T> y) {
        x = std::move(y.inner);
      },
    });
  }

  template <class T>
  void add(std::string_view names, T& x, std::string meta) {
    options_.push_back(option_t{
      split_names(names),
      std::move(meta),
      [&x](located<T> y) {
        x = std::move(y.inner);
      },
    });
  }

  template <class T>
  void
  add(std::string_view names, std::optional<located<T>>& x, std::string meta) {
    options_.push_back(option_t{
      split_names(names),
      std::move(meta),
      [&x](located<T> y) {
        x = std::move(y);
      },
    });
  }

  void add(std::string_view names, bool& x) {
    options_.push_back(option_t{
      split_names(names),
      "",
      [&x](located<std::monostate>) {
        x = true;
      },
    });
  }

  void add(std::string_view names, std::optional<location>& x) {
    options_.push_back(option_t{
      split_names(names),
      "",
      [&x](located<std::monostate> y) {
        x = y.source;
      },
    });
  }

  void parse(parser_interface& p);

  auto usage() const -> std::string;

private:
  static auto split_names(std::string_view names) -> std::vector<std::string> {
    auto result = detail::to_strings(detail::split(names, ","));
    for (auto& name : result) {
      VAST_ASSERT(name.starts_with("-"));
    }
    return result;
  }

  void parse_impl(parser_interface& p) const;

  template <class T>
  using setter = std::function<void(located<T>)>;

  struct positional_t {
    std::string meta;
    std::variant<setter<std::string>, setter<vast::expression>,
                 setter<tql::expression>, setter<size_t>>
      set;
  };

  struct option_t {
    std::vector<std::string> names;
    std::string meta;
    std::variant<setter<std::string>, setter<std::monostate>> set;
  };

  bool called_parse_ = false;
  std::vector<positional_t> positional_;
  std::optional<std::size_t> first_optional_;
  std::vector<option_t> options_;
  std::string name_;
  std::string docs_;
};

} // namespace vast::tql
