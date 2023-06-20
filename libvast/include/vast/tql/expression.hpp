//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/data.hpp"
#include "vast/detail/inspect_enum_str.hpp"
#include "vast/offset.hpp"
#include "vast/tql/basic.hpp"
#include "vast/type.hpp"
#include "vast/value_ptr.hpp"
#include "vast/variant.hpp"

#include <fmt/format.h>

#include <memory>
#include <string>
#include <variant>

namespace vast::tql {

enum class binary_op { equals, not_equals, add, mul };

auto inspect(auto& f, binary_op& x) {
  return detail::inspect_enum_str(f, x, {"equals", "not_equals", "add", "mul"});
}

struct binary_expr {
  value_ptr<expression> left;
  binary_op op{};
  location op_source;
  value_ptr<expression> right;

  auto operator==(const binary_expr&) const -> bool = default;

  friend auto inspect(auto& f, binary_expr& x) {
    return f.object(x)
      .pretty_name("binary_expr")
      .fields(f.field("left", x.left), f.field("op", x.op),
              f.field("op_source", x.op_source), f.field("right", x.right));
  }
};

struct star_projection {
  location source;

  auto operator==(const star_projection&) const -> bool = default;

  friend auto inspect(auto& f, star_projection& x) {
    return f.object(x)
      .pretty_name("star_projection")
      .fields(f.field("source", x.source));
  }
};

struct projection
  : variant<identifier, located<type>, star_projection, located<int64_t>> {
  using variant::variant;

  auto source() const -> location {
    return match([](const auto& x) {
      return x.source;
    });
  }

  friend auto inspect(auto& f, projection& x) {
    // TODO: This overload should not be necessary. However, without the
    // additional object wrapping, the JSON reader is led to an unexpected state
    // after the variant has been read.
    return f.object(x)
      .pretty_name("projection")
      .fields(f.field("kind", static_cast<variant&>(x)));
  }
};

struct extractor {
  std::vector<projection> path;
  location source;

  auto operator==(const extractor&) const -> bool = default;

  // Returns `true` for non-empty sequences of fields.
  auto is_field_path() const -> bool {
    if (path.empty()) {
      return false;
    }
    return std::all_of(path.begin(), path.end(), [](auto& proj) {
      return std::holds_alternative<identifier>(proj);
    });
  }

  /// Returns `true` if this extractor references the root object.
  auto is_root() const -> bool {
    return path.empty();
  }

  friend auto inspect(auto& f, extractor& x) {
    return f.object(x)
      .pretty_name("extractor")
      .fields(f.field("path", x.path), f.field("source", x.source));
  }
};

enum class meta_extractor { schema };

auto inspect(auto& f, meta_extractor& x) {
  return detail::inspect_enum_str(f, x, {"schema"});
}

struct call_expr {
  identifier function;
  std::vector<expression> args;

  auto operator==(const call_expr&) const -> bool = default;

  friend auto inspect(auto& f, call_expr& x) {
    return f.object(x)
      .pretty_name("call_expr")
      .fields(f.field("function", x.function), f.field("args", x.args));
  }
};

/// Before being bound, an expression does not contain `offset`. After being
/// bound, it does not contian `extractor`, `meta_extractor` and `type_extractor`.
struct expression
  : variant<data, binary_expr, extractor, meta_extractor, offset, call_expr> {
  location source;

  auto operator==(const expression&) const -> bool = default;

  friend auto inspect(auto& f, expression& x) {
    return f.object(x)
      .pretty_name("expression")
      .fields(f.field("kind", static_cast<variant&>(x)),
              f.field("source", x.source));
  }
};

} // namespace vast::tql

template <>
struct fmt::formatter<vast::tql::projection> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::tql::projection& x, FormatContext& ctx) const {
    using namespace vast;

    return x.match(
      [&](const identifier& y) {
        return fmt::format_to(ctx.out(), "{}", y.name);
      },
      [&](const located<vast::type>& y) {
        // TODO
        return fmt::format_to(ctx.out(), ":{}", y.inner);
      },
      [&](tql::star_projection) {
        return fmt::format_to(ctx.out(), "*");
      },
      [&](located<int64_t> y) {
        return fmt::format_to(ctx.out(), "[{}]", y.inner);
      });
  }
};

template <>
struct fmt::formatter<vast::tql::extractor> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::tql::extractor& x, FormatContext& ctx) const {
    using namespace vast;

    if (x.path.empty()) {
      return fmt::format_to(ctx.out(), ".");
    }
    auto first = true;
    auto out = ctx.out();
    for (auto& proj : x.path) {
      if (std::exchange(first, false)) {
        if (!std::holds_alternative<located<type>>(proj)
            && !std::holds_alternative<identifier>(proj)) {
          out = fmt::format_to(out, ".");
        }
      }
      out = fmt::format_to(out, "{}", proj);
    }
    return out;
  }
};

template <>
struct fmt::formatter<vast::tql::call_expr> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::tql::call_expr& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{}({})", x.function,
                          fmt::join(x.args, ", "));
  }
};

template <>
struct fmt::formatter<vast::tql::meta_extractor> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::tql::meta_extractor& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{}", std::invoke([&] {
                            using enum vast::tql::meta_extractor;
                            switch (x) {
                              case schema:
                                return "#schema";
                            }
                            VAST_UNREACHABLE();
                          }));
  }
};

template <>
struct fmt::formatter<vast::tql::binary_expr> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::tql::binary_expr& x, FormatContext& ctx) const {
    VAST_ASSERT(x.left);
    VAST_ASSERT(x.right);
    return fmt::format_to(ctx.out(), "({} {} {})", *x.left, x.op, *x.right);
  }
};

template <>
struct fmt::formatter<vast::tql::expression> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::tql::expression& x, FormatContext& ctx) const {
    return x.match(
      [&](const std::string& y) {
        // TODO: Escaping.
        return fmt::format_to(ctx.out(), "\"{}\"", y);
      },
      [&](const vast::offset& y) {
        return fmt::format_to(ctx.out(), "offset{}", y);
      },
      [&](const auto& y) {
        // TODO: Why does removing this not work?
        return fmt::format_to(ctx.out(), fmt::runtime("{}"), y);
      });
  }
};

template <>
struct fmt::formatter<vast::tql::binary_op> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::tql::binary_op& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{}", std::invoke([&] {
                            using enum vast::tql::binary_op;
                            switch (x) {
                              case equals:
                                return "==";
                              case not_equals:
                                return "!=";
                              case add:
                                return "+";
                              case mul:
                                return "*";
                            }
                            VAST_UNREACHABLE();
                          }));
  }
};
