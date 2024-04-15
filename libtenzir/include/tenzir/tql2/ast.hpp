//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/location.hpp"
#include "tenzir/tql2/entity_id.hpp"

#include <caf/detail/is_one_of.hpp>

#include <type_traits>

namespace tenzir::detail {

/// This function makes a value dependant on the type paramater `T` and can
/// therefore be used to guide instantiation, for example, to prevent early
/// instantiation of incomplete types.
template <class T, class U>
auto make_dependent(U&& x) -> U&& {
  return std::forward<U>(x);
}

} // namespace tenzir::detail

namespace tenzir::tql2::ast {

struct assignment;
struct binary_expr;
struct dollar_var;
struct entity;
struct expression;
struct field_access;
struct function_call;
struct identifier;
struct if_stmt;
struct index_expr;
struct invocation;
struct let_stmt;
struct list;
struct literal;
struct match_stmt;
struct null;
struct pipeline_expr;
struct pipeline;
struct record;
struct selector;
struct unary_expr;
struct underscore;
struct unpack;

struct identifier {
  identifier() = default;

  identifier(std::string name, location location)
    : name{std::move(name)}, location{location} {
  }

  identifier(std::string_view name, location location)
    : identifier{std::string(name), location} {
  }

  std::string name;
  tenzir::location location;

  friend auto inspect(auto& f, identifier& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->fmt_value("`{}`", x.name)
             && dbg->append(" @ {:?}", x.location);
    }
    return f.object(x).fields(f.field("symbol", x.name),
                              f.field("location", x.location));
  }
};

struct selector {
  selector() = default;

  selector(std::optional<location> this_, std::vector<identifier> path)
    : this_{this_}, path{std::move(path)} {
  }

  // TODO
  std::optional<location> this_;
  std::vector<identifier> path;

  auto get_location() const -> location {
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

struct underscore : location {
  auto get_location() const -> location {
    return *this;
  }
};

struct dollar_var : identifier {
  auto get_location() const -> tenzir::location {
    return identifier::location;
  }
};

struct null {};

struct literal {
  // TODO: Consider moving the location into the variant inhabitants.
  // TODO: Consider representing integers differently.
  using kind = variant<null, bool, int64_t, uint64_t, double, std::string, blob,
                       duration, caf::timestamp, ip>;

  literal() = default;

  literal(kind value, location source)
    : value{std::move(value)}, source{source} {
  }

  kind value;
  location source;

  friend auto inspect(auto& f, literal& x) -> bool {
    return f.object(x).fields(f.field("value", x.value),
                              f.field("source", x.source));
  }

  auto as_data() const -> data {
    return value.match(
      [](const auto& x) -> data {
        return x;
      },
      [](const null&) -> data {
        return caf::none;
      });
  }

  auto get_location() const -> location {
    return source;
  }
};

using expression_kinds
  = caf::detail::type_list<record, list, selector, pipeline_expr, literal,
                           field_access, index_expr, binary_expr, unary_expr,
                           function_call, underscore, unpack, assignment,
                           dollar_var>;

using expression_kind = caf::detail::tl_apply_t<expression_kinds, variant>;

struct expression {
  expression() = default;

  template <class T>
    requires(
      caf::detail::tl_contains<expression_kinds, std::remove_cvref_t<T>>::value)
  explicit(false) expression(T&& x)
    : kind{std::make_unique<expression_kind>(std::forward<T>(x))} {
  }

  ~expression();
  expression(const expression&) = delete;
  expression(expression&&) noexcept;
  auto operator=(const expression&) -> expression& = delete;
  auto operator=(expression&&) noexcept -> expression&;

  std::unique_ptr<expression_kind> kind;

  template <class Inspector>
  friend auto inspect(Inspector& f, expression& x) -> bool {
    if constexpr (Inspector::is_loading) {
      x.kind = std::make_unique<expression_kind>();
    } else {
      if (auto dbg = as_debug_writer(f);
          dbg && not detail::make_dependent<Inspector>(x.kind)) {
        return dbg->fmt_value("<invalid>");
      }
      TENZIR_ASSERT(x.kind);
    }
    return f.apply(*detail::make_dependent<Inspector>(x.kind));
  }

  template <class... Fs>
  auto match(Fs&&... fs) & -> decltype(auto);
  template <class... Fs>
  auto match(Fs&&... fs) && -> decltype(auto);
  template <class... Fs>
  auto match(Fs&&... fs) const& -> decltype(auto);
  template <class... Fs>
  auto match(Fs&&... fs) const&& -> decltype(auto);

  auto get_location() const -> location;
};

struct unpack {
  unpack() = default;

  unpack(expression expr, location brackets)
    : expr{std::move(expr)}, brackets{brackets} {
  }

  expression expr;
  location brackets;

  friend auto inspect(auto& f, unpack& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr),
                              f.field("brackets", x.brackets));
  }

  auto get_location() const -> location {
    return expr.get_location().combine(brackets);
  }
};

TENZIR_ENUM(binary_op, add, sub, mul, div, eq, neq, gt, ge, lt, le, and_, or_,
            in);

struct binary_expr {
  binary_expr() = default;

  binary_expr(expression left, located<binary_op> op, expression right)
    : left{std::move(left)}, op{op}, right{std::move(right)} {
  }

  expression left;
  located<binary_op> op;
  expression right;

  friend auto inspect(auto& f, binary_expr& x) -> bool {
    return f.object(x).fields(f.field("left", x.left), f.field("op", x.op),
                              f.field("right", x.right));
  }

  auto get_location() const -> location {
    return left.get_location().combine(right.get_location());
  }
};

TENZIR_ENUM(unary_op, pos, neg, not_);

struct unary_expr {
  unary_expr() = default;

  unary_expr(located<unary_op> op, expression expr)
    : op{op}, expr{std::move(expr)} {
  }

  located<unary_op> op;
  expression expr;

  friend auto inspect(auto& f, unary_expr& x) -> bool {
    return f.object(x).fields(f.field("op", x.op), f.field("expr", x.expr));
  }

  auto get_location() const -> location {
    return op.source.combine(expr.get_location());
  }
};

struct assignment {
  assignment() = default;

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

  auto get_location() const -> location {
    return left.get_location().combine(right.get_location());
  }
};

struct entity {
  entity() = default;

  explicit entity(std::vector<identifier> path) : path{std::move(path)} {
  }

  std::vector<identifier> path;
  entity_path ref;

  friend auto inspect(auto& f, entity& x) -> bool {
    return f.object(x).fields(f.field("path", x.path), f.field("ref", x.ref));
  }

  auto get_location() const -> location {
    if (path.empty()) {
      return location::unknown;
    }
    return path.front().location.combine(path.back().location);
  }
};

struct function_call {
  function_call() = default;

  function_call(std::optional<expression> subject, entity fn,
                std::vector<expression> args, location rpar)
    : subject{std::move(subject)},
      fn{std::move(fn)},
      args(std::move(args)),
      rpar{rpar} {
  }

  std::optional<expression> subject;
  entity fn;
  std::vector<expression> args;
  location rpar;

  friend auto inspect(auto& f, function_call& x) -> bool {
    return f.object(x).fields(f.field("subject", x.subject),
                              f.field("fn", x.fn), f.field("args", x.args));
  }

  auto get_location() const -> location {
    auto left = location{};
    if (subject) {
      left = subject->get_location();
    } else {
      left = fn.get_location();
    }
    return left.combine(rpar);
  }
};

struct field_access {
  field_access() = default;

  field_access(expression left, location dot, identifier name)
    : left{std::move(left)}, dot{dot}, name{std::move(name)} {
  }

  expression left;
  location dot;
  identifier name;

  friend auto inspect(auto& f, field_access& x) -> bool {
    return f.object(x).fields(f.field("left", x.left), f.field("dot", x.dot),
                              f.field("name", x.name));
  }

  auto get_location() const -> location {
    return left.get_location().combine(name.location);
  }
};

struct index_expr {
  index_expr() = default;

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

  auto get_location() const -> location {
    return expr.get_location().combine(rbracket);
  }
};

struct list {
  list() = default;

  list(location begin, std::vector<expression> items, location end)
    : begin{begin}, items(std::move(items)), end{end} {
  }

  location begin;
  std::vector<expression> items;
  location end;

  friend auto inspect(auto& f, list& x) -> bool {
    return f.object(x).fields(f.field("begin", x.begin),
                              f.field("items", x.items), f.field("end", x.end));
  }

  auto get_location() const -> location {
    return begin.combine(end);
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
    field() = default;

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

  record() = default;

  record(location begin, std::vector<content_kind> content, location end)
    : begin{begin}, content{std::move(content)}, end{end} {
  }

  location begin;
  std::vector<content_kind> content;
  location end;

  friend auto inspect(auto& f, record& x) -> bool {
    return f.object(x).fields(f.field("begin", x.begin),
                              f.field("content", x.content),
                              f.field("end", x.end));
  }

  auto get_location() const -> location {
    return tenzir::location{begin.begin, end.end};
  }
};

struct invocation {
  invocation() = default;

  invocation(entity op, std::vector<expression> args)
    : op{std::move(op)}, args(std::move(args)) {
  }

  entity op;
  std::vector<expression> args;

  friend auto inspect(auto& f, invocation& x) -> bool {
    return f.object(x).fields(f.field("op", x.op), f.field("args", x.args));
  }
};

using statement
  = variant<invocation, assignment, let_stmt, if_stmt, match_stmt>;

struct pipeline {
  pipeline() = default;
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
  let_stmt() = default;

  let_stmt(location let, identifier name, expression expr)
    : let{let}, name{std::move(name)}, expr{std::move(expr)} {
  }

  location let;
  identifier name;
  expression expr;

  friend auto inspect(auto& f, let_stmt& x) -> bool {
    return f.object(x).fields(f.field("let", x.let), f.field("name", x.name),
                              f.field("expr", x.expr));
  }

  auto get_location() const -> location {
    return let.combine(expr.get_location());
  }
};

struct if_stmt {
  if_stmt() = default;

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

  auto get_location() const -> location {
    // TODO
    return condition.get_location();
  }
};

struct match_stmt {
  struct arm {
    std::vector<expression> filter;
    pipeline pipe;

    friend auto inspect(auto& f, arm& x) -> bool {
      return f.object(x).fields(f.field("filter", x.filter),
                                f.field("pipe", x.pipe));
    }
  };

  match_stmt() = default;

  match_stmt(expression expr, std::vector<arm> arms)
    : expr{std::move(expr)}, arms{std::move(arms)} {
  }

  expression expr;
  std::vector<arm> arms;

  friend auto inspect(auto& f, match_stmt& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr), f.field("arms", x.arms));
  }

  auto get_location() const -> location {
    // TODO
    return expr.get_location();
  }
};

struct pipeline_expr {
  pipeline_expr() = default;

  pipeline_expr(location begin, pipeline inner, location end)
    : begin{begin}, inner{std::move(inner)}, end{end} {
  }

  location begin;
  pipeline inner;
  location end;

  friend auto inspect(auto& f, pipeline_expr& x) -> bool {
    return f.object(x).fields(f.field("begin", x.begin),
                              f.field("inner", x.inner), f.field("end", x.end));
  }

  auto get_location() const -> location {
    return begin.combine(end);
  }
};

inline expression::~expression() = default;
inline expression::expression(expression&&) noexcept = default;
inline auto expression::operator=(expression&&) noexcept
  -> expression& = default;

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

template <class Self>
class visitor {
public:
  void enter(pipeline& x) {
    go(x.body);
  }

  void enter(statement& x) {
    match(x);
  }

  void enter(assignment& x) {
    go(x.left);
    go(x.right);
  }

  void enter(invocation& x) {
    go(x.op);
    go(x.args);
  }

  void enter(if_stmt& x) {
    go(x.condition);
    go(x.then);
    if (x.else_) {
      go(*x.else_);
    }
  }

  void enter(entity& x) {
    (void)x;
  }

  void enter(expression& x) {
    match(x);
  }

  void enter(selector& x) {
    (void)x;
  }

  void enter(binary_expr& x) {
    go(x.left);
    go(x.right);
  }

  void enter(unary_expr& x) {
    go(x.expr);
  }

  void enter(literal& x) {
    (void)x;
  }

  void enter(function_call& x) {
    if (x.subject) {
      go(x.subject);
    }
    go(x.fn);
    go(x.args);
  }

  void enter(pipeline_expr& x) {
    go(x.inner);
  }

  void enter(record& x) {
    for (auto& y : x.content) {
      y.match(
        [&](record::field& z) {
          go(z.expr);
        },
        [&](record::spread& z) {
          go(z.expr);
        });
    }
  }

  void enter(list& x) {
    go(x.items);
  }

  void enter(field_access& x) {
    go(x.left);
  }

  template <class T>
  void enter(T&) {
    TENZIR_WARN("missed {}", typeid(T).name());
  }

private:
  template <class T>
  void match(T& x) {
    x.match([&](auto& y) {
      self().visit(y);
    });
  }

  template <class T>
  void go(T& x) {
    if constexpr (std::ranges::range<T>) {
      for (auto& y : x) {
        self().visit(y);
      }
    } else {
      self().visit(x);
    }
  }

  auto self() -> Self {
    return static_cast<Self&>(*this);
  }
};

} // namespace tenzir::tql2::ast

namespace tenzir {

template <>
inline constexpr auto enable_default_formatter<tenzir::tql2::ast::pipeline>
  = true;

template <>
inline constexpr auto enable_default_formatter<tenzir::tql2::ast::expression>
  = true;

} // namespace tenzir
