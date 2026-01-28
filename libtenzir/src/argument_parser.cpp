//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser.hpp"

#include "tenzir/concept/convertible/to.hpp"
#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/pipeline.hpp"

#include <string_view>
#include <variant>

namespace tenzir {

void argument_parser::parse(parser_interface& p) {
  called_parse_ = true;
  try {
    parse_impl(p);
  } catch (diagnostic& d) {
    d.notes.push_back(diagnostic_note{diagnostic_note_kind::usage, usage()});
    if (!docs_.empty()) {
      d.notes.push_back(diagnostic_note{diagnostic_note_kind::docs, docs_});
    }
    throw std::move(d);
  }
}

void argument_parser::parse_impl(parser_interface& p) const {
  // We resolve the ambiguity between `[sort] -x` and `[file] -f` by
  // not allowing short options if there is a positional expression.
  auto has_positional_expression = std::any_of(
    positional_.begin(), positional_.end(), [](const positional_t& p) {
      return std::holds_alternative<setter<tql::expression>>(p.set)
             || std::holds_alternative<setter<tenzir::expression>>(p.set);
    });
  if (has_positional_expression) {
    for (const auto& option : named_) {
      for (const auto& name : option.names) {
        TENZIR_DIAG_ASSERT(name.starts_with("--"));
      }
    }
  }
  auto positional = size_t{0};
  while (!p.at_end()) {
    auto arg = p.peek_shell_arg();
    if (!arg) {
      diagnostic::error("expected shell-like argument")
        .primary(p.current_span())
        .throw_();
    }
    auto is_option = (arg->inner.size() > 2 && arg->inner.starts_with("--"))
                     || (arg->inner.size() > 1 && arg->inner.starts_with("-")
                         && !has_positional_expression);
    if (is_option) {
      TENZIR_DIAG_ASSERT(arg == p.accept_shell_arg());
      auto split = detail::split(arg->inner, "=", 1);
      auto name = located<std::string_view>{};
      auto value = std::optional<located<std::string_view>>{};
      if (split.size() == 1) {
        name.inner = arg->inner;
        name.source = arg->source;
      } else {
        TENZIR_DIAG_ASSERT(split.size() == 2);
        name.inner = split[0];
        value.emplace();
        value->inner = split[1];
        if (arg->source != location::unknown) {
          // TODO: These locations are not necessarily correct for quoted options.
          name.source.begin = arg->source.begin;
          name.source.end = name.source.begin + split[0].size();
          value->source.end = arg->source.end;
          value->source.begin = value->source.end - split[1].size();
        }
      }
      auto it
        = std::find_if(named_.begin(), named_.end(), [&](const named_t& o) {
            return std::find(o.names.begin(), o.names.end(), name.inner)
                   != o.names.end();
          });
      if (it == named_.end()) {
        diagnostic::error("unknown option `{}`", name.inner)
          .primary(name.source)
          .throw_();
      }
      auto f = detail::overload{
        [&](const setter<std::monostate>& set) {
          if (value) {
            diagnostic::error("unexpected value to option `{}`", name.inner)
              .primary(value->source)
              .throw_();
          }
          set(located<std::monostate>{std::monostate{}, name.source});
        },
        [&](const setter<std::string>& set) {
          if (value) {
            set(located{std::string{value->inner}, value->source});
          } else {
            if (auto value = p.accept_shell_arg()) {
              set(std::move(*value));
            } else {
              diagnostic::error("expected argument after `{}`", name.inner)
                .primary(p.current_span())
                .throw_();
            }
          }
        },
      };
      std::visit(f, it->set);
    } else if (positional >= positional_.size()) {
      auto arg = p.accept_shell_arg();
      auto source = arg ? arg->source : p.current_span();
      diagnostic::error("unexpected positional argument")
        .primary(source)
        .throw_();
    } else {
      auto f = detail::overload{
        [&](const setter<std::string>& set) {
          if (auto arg = p.accept_shell_arg()) {
            set(std::move(*arg));
          } else {
            diagnostic::error("expected positional argument")
              .primary(p.current_span())
              .throw_();
          }
        },
        [&](const setter<tql::expression>& set) {
          auto expr = p.parse_expression();
          auto source = expr.source;
          set({std::move(expr), source});
        },
        [&](const setter<tenzir::expression>& set) {
          set(p.parse_legacy_expression());
        },
        [&](const setter<uint64_t>& set) {
          auto arg = p.accept_shell_arg();
          if (!arg) {
            diagnostic::error("expected positional argument")
              .primary(p.current_span())
              .throw_();
          }
          auto it = arg->inner.begin();
          auto parsed = parsers::count.apply(it, arg->inner.end());
          if (!parsed || it != arg->inner.end()) {
            diagnostic::error("expected a number").primary(arg->source).throw_();
          }
          set({*parsed, arg->source});
        },
      };
      std::visit(f, positional_[positional].set);
      positional += 1;
    }
  }
  TENZIR_DIAG_ASSERT(positional <= positional_.size());
  auto required = first_optional_ ? *first_optional_ : positional_.size();
  if (positional < required) {
    diagnostic::error("expected {} positional arguments, but got {}",
                      positional_.size(), positional)
      .primary(p.current_span())
      .throw_();
  }
}

auto argument_parser::usage() const -> std::string {
  if (positional_.empty() && named_.empty()) {
    return fmt::format("{} (takes no arguments)", name_);
  }
  auto result = std::string{name_};
  auto out = std::back_inserter(result);
  for (const auto& p : positional_) {
    auto index = detail::narrow<size_t>(&p - positional_.data());
    if (!first_optional_ || index < *first_optional_) {
      fmt::format_to(out, " {}", p.meta);
    } else {
      fmt::format_to(out, " [{}]", p.meta);
    }
  }
  for (const auto& o : named_) {
    if (std::holds_alternative<setter<std::monostate>>(o.set)) {
      fmt::format_to(out, " [{}]", fmt::join(o.names, "|"));
    } else {
      fmt::format_to(out, " [{} {}]", fmt::join(o.names, "|"), o.meta);
    }
  }
  return result;
}

} // namespace tenzir
