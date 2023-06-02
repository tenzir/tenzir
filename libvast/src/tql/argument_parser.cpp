//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/tql/argument_parser.hpp"

#include "vast/concept/convertible/data.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"

#include <variant>

namespace vast::tql {

void argument_parser::parse(parser_interface& p) {
  called_parse_ = true;
  try {
    parse_impl(p);
  } catch (diagnostic d) {
    d.notes.push_back(
      diagnostic_note{diagnostic_note_kind::usage, usage(), {}});
    if (!docs_.empty()) {
      d.notes.push_back(diagnostic_note{diagnostic_note_kind::docs, docs_, {}});
    }
    throw std::move(d);
  }
}

void argument_parser::parse_impl(parser_interface& p) const {
  // We resolve the ambiguity between `[sort] -x` and `[file] -f` by
  // not allowing short options if there is a positional expression.
  auto has_positional_expression = std::any_of(
    positional_.begin(), positional_.end(), [](const positional_t& p) {
      return std::holds_alternative<setter<tql::expression>>(p.set)
             || std::holds_alternative<setter<vast::expression>>(p.set);
    });
  if (has_positional_expression) {
    for (const auto& option : options_) {
      for (const auto& name : option.names) {
        VAST_ASSERT(name.starts_with("--"));
      }
    }
  }
  auto positional = size_t{0};
  while (!p.at_end()) {
    auto option = p.accept_long_option();
    if (!option && !has_positional_expression) {
      option = p.accept_short_option();
    }
    if (option) {
      auto split = detail::split(option->inner, "=", "", 1);
      VAST_ASSERT(!split.empty());
      auto name = split[0];
      auto it = std::find_if(
        options_.begin(), options_.end(), [&](const option_t& o) {
          return std::find(o.names.begin(), o.names.end(), name)
                 != o.names.end();
        });
      if (it == options_.end()) {
        // TODO: did you mean...
        diagnostic::error("unknown option `{}`", name)
          .primary(option->source)
          .throw_();
      }
      auto f = detail::overload{
        [&](const setter<std::monostate>& set) {
          set(located<std::monostate>{std::monostate{}, option->source});
        },
        [&](const setter<std::string>& set) {
          if (split.size() == 2) {
            // TODO: Implement this in a better way.
            set(located{std::string{split[1]}, option->source});
          } else {
            if (auto arg = p.accept_shell_arg()) {
              set(std::move(*arg));
            } else {
              diagnostic::error("expected argument after `{}`", option->inner)
                .primary(p.current_span())
                .throw_();
            }
          }
        },
      };
      std::visit(f, it->set);
    } else if (positional >= positional_.size()) {
      diagnostic::error("unexpected positional argument")
        .primary(p.current_span())
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
        [&](const setter<vast::expression>& set) {
          set(p.parse_legacy_expression());
        },
        [&](const setter<size_t>& set) {
          // TODO: This matches shell behavior, but do we want that?
          auto arg = p.accept_shell_arg();
          if (!arg) {
            diagnostic::error("expected positional argument")
              .primary(p.current_span())
              .throw_();
          }
          // TODO
          auto it = arg->inner.begin();
          auto parsed = parsers::u64.apply(it, arg->inner.end());
          if (!parsed || it != arg->inner.end()) {
            diagnostic::error("expected an integer literal")
              .primary(arg->source)
              .throw_();
          }
          set({*parsed, arg->source});
        },
      };
      std::visit(f, positional_[positional].set);
      positional += 1;
    }
  }
  VAST_ASSERT(positional <= positional_.size());
  auto required = first_optional_ ? *first_optional_ : positional_.size();
  if (positional < required) {
    // TODO: diagnostic with optional
    throw diagnostic{
      severity::error,
      fmt::format("expected {} positional "
                  "arguments, but got {}",
                  positional_.size(), positional),
      {diagnostic_annotation{true, "", p.current_span()}},
      {},
    };
  }
}

auto argument_parser::usage() const -> std::string {
  if (positional_.empty() && options_.empty()) {
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
  for (const auto& o : options_) {
    if (std::holds_alternative<setter<std::monostate>>(o.set)) {
      fmt::format_to(out, " [{}]", fmt::join(o.names, "|"));
    } else {
      fmt::format_to(out, " [{} {}]", fmt::join(o.names, "|"), o.meta);
    }
  }
  return result;
}

} // namespace vast::tql
