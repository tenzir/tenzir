//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/location.hpp"

namespace tenzir::detail {

// TODO
template <class T = void, class U>
auto identity(U&& x) -> U&& {
  return std::forward<U>(x);
}

} // namespace tenzir::detail

namespace tenzir::tql2::ast {

struct assignment;
struct binary_expr;
struct expression;
struct field_access;
struct function_call;
struct identifier;
struct if_stmt;
struct index_expr;
struct invocation;
struct let_stmt;
struct list;
struct match_stmt;
struct pipeline_expr;
struct pipeline;
struct record;
struct selector;
struct unary_expr;
struct unpack;

struct identifier {
  identifier(std::string name, location location)
    : name{std::move(name)}, location{location} {
  }

  identifier(std::string_view name, location location)
    : identifier{std::string(name), location} {
  }

  std::string name;
  location location;

  friend auto inspect(auto& f, identifier& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->apply(x.name) && dbg->append(" @ {:?}", x.location);
    }
    return f.object(x).fields(f.field("symbol", x.name),
                              f.field("location", x.location));
  }
};

struct selector {
  selector(std::optional<location> this_, std::vector<identifier> path)
    : this_{this_}, path{std::move(path)} {
  }

  // TODO
  std::optional<location> this_;
  std::vector<identifier> path;

  auto location() const -> location {
    auto result = tenzir::location{};
    if (this_) {
      result.begin = this_->begin;
      result.end = this_->end;
    } else {
      TENZIR_ASSERT(not path.empty());
      result.begin = path.front().location.begin;
    }
    if (not path.empty()) {
      result.end = path.back().location.end;
    }
    return result;
  }

  friend auto inspect(auto& f, selector& x) -> bool {
    return f.object(x).fields(f.field("this", x.this_),
                              f.field("path", x.path));
  }
};

struct string : located<std::string> {
  using located::located;
};

struct integer : located<std::string> {
  explicit integer(located<std::string> x) : located{std::move(x)} {
  }

  using located::located;
};

struct boolean : located<bool> {
  using located::located;
};

struct null : location {
  auto location() const -> location {
    return *this;
  }
};

struct duration : located<tenzir::duration> {};

struct underscore : location {
  auto location() const -> location {
    return *this;
  }
};

using expression_kind
  = variant<record, list, selector, pipeline_expr, string, integer, boolean,
            field_access, index_expr, binary_expr, unary_expr, function_call,
            null, underscore, unpack>;

struct expression {
  template <class T>
  explicit(false) expression(T&& x)
    : kind{std::make_unique<expression_kind>(std::forward<T>(x))} {
  }

  ~expression() = default;
  expression(const expression&) = delete;
  expression(expression&&) = default;
  auto operator=(const expression&) -> expression& = delete;
  auto operator=(expression&&) -> expression& = default;

  // explicit expression(record x);

  template <class... Fs>
  auto match(Fs&&... fs) & -> decltype(auto);
  template <class... Fs>
  auto match(Fs&&... fs) && -> decltype(auto);
  template <class... Fs>
  auto match(Fs&&... fs) const& -> decltype(auto);
  template <class... Fs>
  auto match(Fs&&... fs) const&& -> decltype(auto);

  auto location() const -> location;

  std::unique_ptr<expression_kind> kind;

  template <class Inspector>
  friend auto inspect(Inspector& f, expression& x) -> bool {
    // TODO
    if constexpr (Inspector::is_loading) {
      x.kind = std::make_unique<expression_kind>();
    } else {
      TENZIR_ASSERT(x.kind);
    }
    return f.apply(*detail::identity<Inspector>(x.kind));
  }
};

struct unpack {
  unpack(expression expr, location brackets)
    : expr{std::move(expr)}, brackets{brackets} {
  }

  expression expr;
  location brackets;

  friend auto inspect(auto& f, unpack& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr),
                              f.field("brackets", x.brackets));
  }

  auto location() const -> location {
    return expr.location().combine(brackets);
  }
};

TENZIR_ENUM(binary_op, add, sub, mul, div, eq, neq, gt, ge, lt, le, and_, or_);

struct binary_expr {
  binary_expr(expression left, located<binary_op> op, expression right)
    : left{std::move(left)}, op{op}, right{std::move(right)} {
  }

  auto location() const -> location {
    return left.location().combine(right.location());
  }

  expression left;
  located<binary_op> op;
  expression right;

  friend auto inspect(auto& f, binary_expr& x) -> bool {
    return f.object(x).fields(f.field("left", x.left), f.field("op", x.op),
                              f.field("right", x.right));
  }
};

TENZIR_ENUM(unary_op, pos, neg, not_);

struct unary_expr {
  unary_expr(located<unary_op> op, expression expr)
    : op{op}, expr{std::move(expr)} {
  }

  auto location() const -> location {
    return op.source.combine(expr.location());
  }

  located<unary_op> op;
  expression expr;

  friend auto inspect(auto& f, unary_expr& x) -> bool {
    return f.object(x).fields(f.field("op", x.op), f.field("expr", x.expr));
  }
};

struct assignment {
  assignment(selector left, location equals, expression right)
    : left{std::move(left)}, equals{equals}, right{std::move(right)} {
  }

  selector left;
  location equals;
  expression right;

  friend auto inspect(auto& f, assignment& x) -> bool {
    return f.object(x).fields(f.field("left", x.left),
                              f.field("equals", x.equals),
                              f.field("right", x.right));
  }

  auto location() const -> location {
    return left.location().combine(right.location());
  }
};

struct argument : variant<expression, assignment> {
  using variant::variant;

  auto location() const -> location {
    return match([](auto& x) {
      return x.location();
    });
  }
};

struct entity {
  explicit entity(std::vector<identifier> path) : path{std::move(path)} {
  }

  std::vector<identifier> path;

  auto location() const -> location {
    if (path.empty()) {
      return location::unknown;
    }
    return path.front().location.combine(path.back().location);
  }

  friend auto inspect(auto& f, entity& x) -> bool {
    return f.object(x).fields(f.field("path", x.path));
  }
};

struct function_call {
  function_call(std::optional<expression> receiver, entity fn,
                std::vector<argument> args)
    : receiver{std::move(receiver)}, fn{std::move(fn)}, args{std::move(args)} {
  }

  std::optional<expression> receiver;
  entity fn;
  std::vector<argument> args;

  auto location() const -> location {
    // TODO
    return fn.location();
  }

  friend auto inspect(auto& f, function_call& x) -> bool {
    return f.object(x).fields(f.field("receiver", x.receiver),
                              f.field("fn", x.fn), f.field("args", x.args));
  }
};

struct field_access {
  field_access(expression left, location dot, identifier name)
    : left{std::move(left)}, dot{dot}, name{std::move(name)} {
  }

  expression left;
  location dot;
  identifier name;

  auto location() const -> location {
    return left.location().combine(name.location);
  }

  friend auto inspect(auto& f, field_access& x) -> bool {
    return f.object(x).fields(f.field("left", x.left), f.field("dot", x.dot),
                              f.field("name", x.name));
  }
};

struct index_expr {
  index_expr(expression expr, location lbracket, expression index,
             location rbracket)
    : expr{std::move(expr)},
      lbracket{lbracket},
      index{std::move(index)},
      rbracket{rbracket} {
  }

  expression expr;
  location lbracket;
  expression index;
  location rbracket;

  friend auto inspect(auto& f, index_expr& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr),
                              f.field("lbracket", x.lbracket),
                              f.field("index", x.index),
                              f.field("rbracket", x.rbracket));
  }

  auto location() const -> location {
    return expr.location().combine(rbracket);
  }
};

struct list {
  list(location open, std::vector<expression> items, location close)
    : open{open}, items{std::move(items)}, close{close} {
  }

  location open;
  std::vector<expression> items;
  location close;

  friend auto inspect(auto& f, list& x) -> bool {
    return f.object(x).fields(f.field("open", x.open),
                              f.field("items", x.items),
                              f.field("close", x.close));
  }

  auto location() const -> location {
    return open.combine(close);
  }
};

struct record {
  struct spread {
    expression expr;

    friend auto inspect(auto& f, spread& x) -> bool {
      return f.object(x).fields(f.field("expr", x.expr));
    }
  };

  struct field {
    field(identifier name, expression expr)
      : name{std::move(name)}, expr{std::move(expr)} {
    }

    identifier name;
    expression expr;

    friend auto inspect(auto& f, field& x) -> bool {
      return f.object(x).fields(f.field("name", x.name),
                                f.field("expr", x.expr));
    }
  };

  using content_kind = variant<field, spread>;

  record(location left, std::vector<content_kind> content, location right)
    : begin{left}, content{std::move(content)}, end{right} {
  }

  location begin;
  std::vector<content_kind> content;
  location end;

  auto location() const -> location {
    return tenzir::location{begin.begin, end.end};
  }

  friend auto inspect(auto& f, record& x) -> bool {
    return f.apply(x.content);
  }
};

struct invocation {
  entity op;
  std::vector<argument> args;

  friend auto inspect(auto& f, invocation& x) -> bool {
    return f.object(x).fields(f.field("op", x.op), f.field("args", x.args));
  }
};

using statement
  = variant<invocation, assignment, let_stmt, if_stmt, match_stmt>;

struct pipeline {
  explicit pipeline(std::vector<statement> body);
  ~pipeline();
  pipeline(const pipeline&) = delete;
  pipeline(pipeline&&) noexcept;
  auto operator=(const pipeline&) -> pipeline& = delete;
  auto operator=(pipeline&&) noexcept -> pipeline&;

  std::vector<statement> body;

  template <class Inspector>
  friend auto inspect(Inspector& f, pipeline& x) -> bool {
    return f.apply(x.body);
  }
};

struct let_stmt {
  let_stmt(identifier name, expression expr)
    : name{std::move(name)}, expr{std::move(expr)} {
  }

  identifier name;
  expression expr;

  friend auto inspect(auto& f, let_stmt& x) -> bool {
    return f.object(x).fields(f.field("name", x.name), f.field("expr", x.expr));
  }
};

struct if_stmt {
  if_stmt(expression condition, pipeline then, std::optional<pipeline> else_)
    : condition{std::move(condition)},
      then{std::move(then)},
      else_{std::move(else_)} {
  }

  expression condition;
  pipeline then;
  std::optional<pipeline> else_;

  friend auto inspect(auto& f, if_stmt& x) -> bool {
    return f.object(x).fields(f.field("condition", x.condition),
                              f.field("then", x.then),
                              f.field("else", x.else_));
  }
};

struct match_case {
  std::vector<expression> filter;
  pipeline pipe;

  friend auto inspect(auto& f, match_case& x) -> bool {
    return f.object(x).fields(f.field("filter", x.filter),
                              f.field("pipe", x.pipe));
  }
};

struct match_stmt {
  expression expr;
  std::vector<match_case> cases;

  friend auto inspect(auto& f, match_stmt& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr),
                              f.field("cases", x.cases));
  }

  match_stmt(expression expr, std::vector<match_case> cases)
    : expr{std::move(expr)}, cases{std::move(cases)} {
  }
};

struct pipeline_expr {
  pipeline_expr(location open, pipeline inner, location close)
    : open{open}, inner{std::move(inner)}, close{close} {
  }

  location open;
  pipeline inner;
  location close;

  auto location() const -> location {
    return open.combine(close);
  }

  friend auto inspect(auto& f, pipeline_expr& x) -> bool {
    return f.object(x).fields(f.field("open", x.open),
                              f.field("inner", x.inner),
                              f.field("close", x.close));
  }
};

template <class... Fs>
auto expression::match(Fs&&... fs) & -> decltype(auto) {
  TENZIR_ASSERT(kind);
  return kind->match(std::forward<Fs>(fs)...);
}
template <class... Fs>
auto expression::match(Fs&&... fs) && -> decltype(auto) {
  TENZIR_ASSERT(kind);
  return kind->match(std::forward<Fs>(fs)...);
}
template <class... Fs>
auto expression::match(Fs&&... fs) const& -> decltype(auto) {
  TENZIR_ASSERT(kind);
  return kind->match(std::forward<Fs>(fs)...);
}
template <class... Fs>
auto expression::match(Fs&&... fs) const&& -> decltype(auto) {
  TENZIR_ASSERT(kind);
  return kind->match(std::forward<Fs>(fs)...);
}

inline pipeline::pipeline(std::vector<statement> body) : body{std::move(body)} {
}
inline pipeline::~pipeline() = default;
inline pipeline::pipeline(pipeline&&) noexcept = default;
inline auto pipeline::operator=(pipeline&&) noexcept -> pipeline& = default;

} // namespace tenzir::tql2::ast

namespace tenzir {

template <>
inline constexpr auto enable_default_formatter<tenzir::tql2::ast::pipeline>
  = true;

template <>
inline constexpr auto enable_default_formatter<tenzir::tql2::ast::expression>
  = true;

} // namespace tenzir
