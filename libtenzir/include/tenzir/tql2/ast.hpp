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
#include "tenzir/detail/type_list.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/let_id.hpp"
#include "tenzir/location.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/variant.hpp"

#include <caf/detail/is_one_of.hpp>

#include <compare>
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

namespace tenzir::ast {

struct identifier {
  identifier() = default;

  template <class T>
    requires std::constructible_from<std::string, T>
  identifier(T&& name, location location)
    : name{std::forward<T>(name)}, location{location} {
  }

  auto get_location() const -> location {
    return location;
  }

  friend auto inspect(auto& f, identifier& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->fmt_value("`{}`", x.name)
             && dbg->append(" @ {:?}", x.location);
    }
    return f.object(x).fields(f.field("symbol", x.name),
                              f.field("location", x.location));
  }

  std::string name;
  tenzir::location location;
};

TENZIR_ENUM(meta_kind, name, import_time, internal);

struct meta {
  using enum meta_kind;

  auto get_location() const -> location {
    return source;
  }

  friend auto inspect(auto& f, meta& x) -> bool {
    return f.object(x).fields(f.field("kind", x.kind),
                              f.field("source", x.source));
  }

  meta_kind kind;
  location source;
};

struct underscore : location {
  auto get_location() const -> location {
    return *this;
  }
};

struct dollar_var {
  dollar_var() = default;

  explicit dollar_var(identifier id) : id{std::move(id)} {
  }

  auto name_without_dollar() const -> std::string_view {
    TENZIR_ASSERT(id.name.starts_with("$"));
    return std::string_view{id.name}.substr(1);
  }

  auto get_location() const -> tenzir::location {
    return id.location;
  }

  friend auto inspect(auto& f, dollar_var& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->fmt_value("`{}`", x.id.name) && dbg->append(" -> {:?}", x.let)
             && dbg->append(" @ {:?}", x.id.location);
    }
    return f.object(x).fields(f.field("id", x.id), f.field("let", x.let));
  }

  identifier id;
  let_id let;
};

struct null {};

struct constant {
  // TODO: Consider moving the location into the variant inhabitants.
  // TODO: Consider representing integers differently.
  using kind
    = detail::tl_apply_t<detail::tl_filter_not_type_t<data::types, pattern>,
                         variant>;

  constant() = default;

  constant(kind value, location source)
    : value{std::move(value)}, source{source} {
  }

  kind value;
  location source;

  friend auto inspect(auto& f, constant& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      if (auto* t = try_as<time>(x.value)) {
        // Time printing is not reliable across platforms otherwise.
        return dbg->fmt_value("time {} @ {:?}", data{*t}, x.source);
      }
      return dbg->fmt_value("{:?} @ {:?}", use_default_formatter{x.value},
                            x.source);
    }
    return f.object(x).fields(f.field("value", x.value),
                              f.field("source", x.source));
  }

  auto as_data() const -> data {
    return value.match([](const auto& x) -> data {
      return x;
    });
  }

  auto get_location() const -> location {
    return source;
  }
};

struct this_ {
  location source;

  auto get_location() const -> location {
    return source;
  }

  friend auto inspect(auto& f, this_& x) -> bool {
    return f.apply(x.source);
  }
};

struct root_field {
  identifier id;
  bool has_question_mark = false;

  auto get_location() const -> location {
    return id.location;
  }

  friend auto inspect(auto& f, root_field& x) -> bool {
    return f.object(x).fields(
      f.field("id", x.id), f.field("has_question_mark", x.has_question_mark));
  }
};

using expression_kinds
  = detail::type_list<record, list, meta, this_, root_field, pipeline_expr,
                      constant, field_access, index_expr, binary_expr,
                      unary_expr, function_call, underscore, unpack, assignment,
                      dollar_var>;

using expression_kind = detail::tl_apply_t<expression_kinds, variant>;

TENZIR_ENUM(substitute_result, no_remaining, some_remaining);

struct expression {
  expression() = default;

  template <class T>
    requires(
      detail::tl_contains<expression_kinds, std::remove_cvref_t<T>>::value)
  explicit(false) expression(T&& x)
    : kind{std::make_unique<expression_kind>(std::forward<T>(x))} {
  }

  ~expression();
  expression(const expression&);
  expression(expression&&) noexcept;
  auto operator=(const expression&) -> expression&;
  auto operator=(expression&&) noexcept -> expression&;

  // TODO: This does not propagate const...
  std::unique_ptr<expression_kind> kind;

  template <class Inspector>
  friend auto inspect(Inspector& f, expression& x) -> bool;

  template <class Result = void, class... Fs>
  auto match(Fs&&... fs) & -> decltype(auto);
  template <class Result = void, class... Fs>
  auto match(Fs&&... fs) && -> decltype(auto);
  template <class Result = void, class... Fs>
  auto match(Fs&&... fs) const& -> decltype(auto);
  template <class Result = void, class... Fs>
  auto match(Fs&&... fs) const&& -> decltype(auto);

  auto get_location() const -> location;

  /// Performs name-resolution for all free `$` variables.
  auto bind(compile_ctx ctx) & -> failure_or<void>;

  /// Partially substitute previously name-resolved variables.
  auto substitute(substitute_ctx ctx) & -> failure_or<substitute_result>;

  /// Returns true if the expression always returns the same value.
  auto is_deterministic(const registry& reg) const -> bool;
};

/// A field path is a list of constant field names.
///
/// This can contain expressions like `foo`, `foo.?bar` and `this.foo["bar"]`.
/// It does not allow `foo[some_expr()]`, `foo[0]`, etc. These field paths will
/// be added at a later point in time.
class field_path {
public:
  struct segment {
    identifier id;
    bool has_question_mark;

    friend auto inspect(auto& f, segment& x) -> bool {
      return f.object(x).fields(
        f.field("id", x.id), f.field("has_question_mark", x.has_question_mark));
    }
  };

  field_path() = default;

  static auto try_from(ast::expression expr) -> std::optional<field_path>;

  template <class... Segments>
  static auto from(located<Segments>... path) -> field_path;

  auto get_location() const -> location {
    return expr_.get_location();
  }

  auto has_this() const -> bool {
    return has_this_;
  }

  auto path() const -> std::span<const segment> {
    return path_;
  }

  auto inner() const -> const ast::expression& {
    return expr_;
  };

  auto unwrap() && -> ast::expression {
    return std::move(expr_);
  }

private:
  field_path(ast::expression expr, bool has_this, std::vector<segment> path)
    : expr_{std::move(expr)}, has_this_{has_this}, path_{std::move(path)} {
  }

  friend auto inspect(auto& f, field_path& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr_),
                              f.field("has_this", x.has_this_),
                              f.field("path", x.path_));
  }

  ast::expression expr_;
  bool has_this_{};
  std::vector<segment> path_;
};

/// A selector is something that can be assigned.
///
/// Note that this is not an actual `expression`. Instead, expressions can be
/// converted to `selector` on-demand. Currently, this is limited to meta
/// selectors (e.g., `@tag`) and simple selectors (see `simple_selector`).
struct selector : variant<meta, field_path> {
  using variant::variant;

  static auto try_from(ast::expression expr) -> std::optional<selector>;

  auto get_location() const -> location {
    return match([](auto& x) {
      return x.get_location();
    });
  }
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

TENZIR_ENUM(binary_op, add, sub, mul, div, eq, neq, gt, geq, lt, leq, and_, or_,
            in, if_, else_);

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
    return left.get_location().combine(right);
  }
};

TENZIR_ENUM(unary_op, pos, neg, not_, move);

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
    return op.source.combine(expr);
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
    return left.get_location().combine(right);
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

  function_call(entity fn, std::vector<expression> args, location rpar,
                bool method)
    : fn{std::move(fn)}, args(std::move(args)), rpar{rpar}, method{method} {
  }

  entity fn;
  std::vector<expression> args;
  location rpar;
  bool method{};

  friend auto inspect(auto& f, function_call& x) -> bool {
    return f.object(x).fields(f.field("fn", x.fn), f.field("args", x.args),
                              f.field("rpar", x.rpar),
                              f.field("method", x.method));
  }

  auto get_location() const -> location {
    auto left = location{};
    if (method) {
      TENZIR_ASSERT(not args.empty());
      left = args[0].get_location();
    } else {
      left = fn.get_location();
    }
    return left.combine(rpar);
  }
};

struct field_access {
  field_access() = default;

  field_access(expression left, location dot, bool has_question_mark,
               identifier name)
    : left{std::move(left)},
      dot{dot},
      has_question_mark{has_question_mark},
      name{std::move(name)} {
  }

  expression left;
  location dot;
  bool has_question_mark = false;
  identifier name;

  friend auto inspect(auto& f, field_access& x) -> bool {
    return f.object(x).fields(f.field("left", x.left), f.field("dot", x.dot),
                              f.field("has_question_mark", x.has_question_mark),
                              f.field("name", x.name));
  }

  auto suppress_warnings() const -> bool {
    return has_question_mark;
  }

  auto get_location() const -> location {
    return left.get_location().combine(name.location);
  }
};

struct index_expr {
  index_expr() = default;

  index_expr(expression expr, location lbracket, expression index,
             location rbracket, bool has_question_mark)
    : expr{std::move(expr)},
      lbracket{lbracket},
      index{std::move(index)},
      rbracket{rbracket},
      has_question_mark{has_question_mark} {
  }

  expression expr;
  location lbracket;
  expression index;
  location rbracket;
  bool has_question_mark = false;

  friend auto inspect(auto& f, index_expr& x) -> bool {
    return f.object(x).fields(
      f.field("expr", x.expr), f.field("lbracket", x.lbracket),
      f.field("index", x.index), f.field("rbracket", x.rbracket),
      f.field("has_question_mark", x.has_question_mark));
  }

  auto get_location() const -> location {
    return expr.get_location().combine(rbracket);
  }
};

struct spread {
  spread() = default;

  spread(location dots, expression expr) : dots{dots}, expr{std::move(expr)} {
  }

  location dots;
  expression expr;

  friend auto inspect(auto& f, spread& x) -> bool {
    return f.object(x).fields(f.field("dots", x.dots), f.field("expr", x.expr));
  }

  auto get_location() const -> location {
    return dots.combine(expr);
  }
};

struct list {
  using item = variant<expression, spread>;

  list() = default;

  list(location begin, std::vector<item> items, location end)
    : begin{begin}, items{std::move(items)}, end{end} {
  }

  location begin;
  std::vector<item> items;
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

  using item = variant<field, spread>;

  record() = default;

  record(location begin, std::vector<item> items, location end)
    : begin{begin}, items{std::move(items)}, end{end} {
  }

  location begin;
  std::vector<item> items;
  location end;

  friend auto inspect(auto& f, record& x) -> bool {
    return f.object(x).fields(f.field("begin", x.begin),
                              f.field("items", x.items), f.field("end", x.end));
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

struct pipeline {
  pipeline() = default;
  explicit pipeline(std::vector<statement> body);
  ~pipeline();
  pipeline(const pipeline&) = default;
  pipeline(pipeline&&) noexcept;
  auto operator=(const pipeline&) -> pipeline& = default;
  auto operator=(pipeline&&) noexcept -> pipeline&;

  std::vector<statement> body;

  template <class Inspector>
  friend auto inspect(Inspector& f, pipeline& x) -> bool {
    return f.apply(x.body);
  }

  auto compile(compile_ctx ctx) && -> failure_or<ir::pipeline>;
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

  auto name_without_dollar() const -> std::string_view {
    TENZIR_ASSERT(name.name.starts_with("$"));
    return std::string_view{name.name}.substr(1);
  }

  auto get_location() const -> location {
    return let.combine(expr);
  }
};

struct if_stmt {
  struct else_t {
    location kw;
    pipeline pipe;

    friend auto inspect(auto& f, else_t& x) -> bool {
      return f.object(x).fields(f.field("kw", x.kw), f.field("pipe", x.pipe));
    }
  };

  if_stmt() = default;

  if_stmt(location if_kw, expression condition, pipeline then,
          std::optional<else_t> else_)
    : if_kw{if_kw},
      condition{std::move(condition)},
      then{std::move(then)},
      else_{std::move(else_)} {
  }

  location if_kw;
  expression condition;
  pipeline then;
  std::optional<else_t> else_;

  friend auto inspect(auto& f, if_stmt& x) -> bool {
    return f.object(x).fields(f.field("if_kw", x.if_kw),
                              f.field("condition", x.condition),
                              f.field("then", x.then),
                              f.field("else", x.else_));
  }

  auto get_location() const -> location {
    // TODO
    return if_kw;
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

  match_stmt(location begin, expression expr, std::vector<arm> arms,
             location end)
    : begin{begin}, expr{std::move(expr)}, arms{std::move(arms)}, end{end} {
  }

  location begin;
  expression expr;
  std::vector<arm> arms;
  location end;

  friend auto inspect(auto& f, match_stmt& x) -> bool {
    return f.object(x).fields(f.field("begin", x.begin),
                              f.field("expr", x.expr), f.field("arms", x.arms),
                              f.field("end", x.end));
  }

  auto get_location() const -> location {
    return begin.combine(end);
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

template <class Result, class... Fs>
auto expression::match(Fs&&... fs) & -> decltype(auto) {
  TENZIR_ASSERT(kind);
  return kind->match<Result>(std::forward<Fs>(fs)...);
}

template <class Result, class... Fs>
auto expression::match(Fs&&... fs) && -> decltype(auto) {
  TENZIR_ASSERT(kind);
  return kind->match<Result>(std::forward<Fs>(fs)...);
}

template <class Result, class... Fs>
auto expression::match(Fs&&... fs) const& -> decltype(auto) {
  TENZIR_ASSERT(kind);
  return kind->match<Result>(std::forward<Fs>(fs)...);
}

template <class Result, class... Fs>
auto expression::match(Fs&&... fs) const&& -> decltype(auto) {
  TENZIR_ASSERT(kind);
  return kind->match<Result>(std::forward<Fs>(fs)...);
}

inline pipeline::pipeline(std::vector<statement> body) : body{std::move(body)} {
}
inline pipeline::~pipeline() = default;
inline pipeline::pipeline(pipeline&&) noexcept = default;
inline auto pipeline::operator=(pipeline&&) noexcept -> pipeline& = default;

template <class... Segments>
auto field_path::from(located<Segments>... path) -> field_path {
  static_assert(sizeof...(path) > 0);
  const auto combine = [](auto&& segment, expression expr) -> expression {
    if constexpr (concepts::integer<decltype(segment.inner)>) {
      return ast::index_expr{
        std::move(expr),
        location::unknown,
        ast::constant{detail::narrow<int64_t>(segment.inner), segment.source},
        location::unknown,
        false,
      };
    } else {
      return ast::field_access{
        std::move(expr),
        location::unknown,
        false,
        ast::identifier{
          std::string{std::forward<decltype(segment)>(segment).inner},
          segment.source},
      };
    }
  };
  const auto recurse
    = [&](const auto& self, auto&& head, auto&&... tail) -> expression {
    if constexpr (sizeof...(tail) == 0) {
      if constexpr (concepts::integer<decltype(head.inner)>) {
        return combine(std::forward<decltype(head)>(head),
                       this_{location::unknown});
      } else {
        return ast::root_field{
          .id = {head.inner, head.source},
          .has_question_mark = false,
        };
      }
    } else {
      return combine(std::forward<decltype(head)>(head),
                     self(self, std::forward<decltype(tail)>(tail)...));
    }
  };
  return check(
    try_from(recurse(recurse, std::forward<decltype(path)>(path)...)));
}

/// AST node visitor with mutable access.
///
/// To use this, define a class like this:
/// ```
/// class my_visitor : public visitor<my_visitor> {
/// public:
///   template<class T>
///   void visit(T& x) {
///     enter(x);
///   }
/// }
/// ```
/// Then override specific `visit` functions. You can remove the template
/// catch-all if you want to ensure that your matching is exhaustive.
template <class Self>
class visitor {
protected:
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
      go(x.else_->pipe);
    }
  }

  void enter(entity& x) {
    go(x.path);
  }

  void enter(expression& x) {
    match(x);
  }

  void enter(binary_expr& x) {
    go(x.left);
    go(x.right);
  }

  void enter(unary_expr& x) {
    go(x.expr);
  }

  void enter(constant& x) {
    TENZIR_UNUSED(x);
  }

  void enter(function_call& x) {
    go(x.fn);
    go(x.args);
  }

  void enter(pipeline_expr& x) {
    go(x.inner);
  }

  void enter(record& x) {
    go(x.items);
  }

  void enter(record::item& x) {
    match(x);
  }

  void enter(record::field& x) {
    go(x.name);
    go(x.expr);
  }

  void enter(spread& x) {
    go(x.expr);
  }

  void enter(list& x) {
    go(x.items);
  }

  void enter(list::item& x) {
    match(x);
  }

  void enter(field_access& x) {
    go(x.left);
  }

  void enter(let_stmt& x) {
    go(x.name);
    go(x.expr);
  }

  void enter(ast::identifier& x) {
    TENZIR_UNUSED(x);
  }

  void enter(ast::meta& x) {
    TENZIR_UNUSED(x);
  }

  void enter(ast::match_stmt& x) {
    go(x.expr);
    go(x.arms);
  }

  void enter(ast::match_stmt::arm& x) {
    go(x.filter);
    go(x.pipe);
  }

  void enter(ast::selector& x) {
    match(x);
  }

  void enter(ast::field_path& x) {
    // TODO: What should we do here?
    TENZIR_UNUSED(x);
  }

  void enter(ast::root_field& x) {
    go(x.id);
  }

  void enter(ast::this_& x) {
    TENZIR_UNUSED(x);
  }

  void enter(ast::dollar_var& x) {
    TENZIR_UNUSED(x);
  }

  void enter(ast::unpack& x) {
    go(x.expr);
  }

  void enter(ast::index_expr& x) {
    go(x.expr);
    go(x.index);
  }

  void enter(ast::underscore& x) {
    TENZIR_UNUSED(x);
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

  template <class T>
  void go(std::optional<T>& x) {
    if (x) {
      go(*x);
    }
  }

  auto self() -> Self& {
    return static_cast<Self&>(*this);
  }
};

template <class Inspector>
auto inspect(Inspector& f, expression& x) -> bool {
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

} // namespace tenzir::ast

namespace tenzir {

template <>
class variant_traits<ast::expression> {
public:
  static constexpr auto count = detail::tl_size<ast::expression_kinds>::value;

  static auto index(const ast::expression& x) -> size_t {
    TENZIR_ASSERT(x.kind);
    return x.kind->index();
  }

  template <size_t I>
  static auto get(const ast::expression& x) -> decltype(auto) {
    return *std::get_if<I>(&*x.kind);
  }
};

auto is_true_literal(const ast::expression& y) -> bool;

/// Partially converts an expression into a legacy expression.
///
/// The return value `(y, z)` satisfies `x <=> y and z`.
auto split_legacy_expression(const ast::expression& x)
  -> std::pair<expression, ast::expression>;

template <>
inline constexpr auto enable_default_formatter<tenzir::ast::pipeline> = true;

template <>
inline constexpr auto enable_default_formatter<tenzir::ast::expression> = true;

} // namespace tenzir
