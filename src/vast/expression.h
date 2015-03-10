#ifndef VAST_EXPRESSION_H
#define VAST_EXPRESSION_H

#include "vast/data.h"
#include "vast/key.h"
#include "vast/offset.h"
#include "vast/operator.h"
#include "vast/type.h"
#include "vast/detail/ast/query.h"
#include "vast/detail/parser/error_handler.h"
#include "vast/detail/parser/skipper.h"
#include "vast/detail/parser/query.h"
#include "vast/expr/normalize.h"
#include "vast/expr/validator.h"
#include "vast/util/variant.h"

namespace vast {

class expression;

/// Extracts the event type.
struct event_extractor : util::totally_ordered<event_extractor>
{
  friend bool operator==(event_extractor const&, event_extractor const&)
  {
    return true;
  }

  friend bool operator<(event_extractor const&, event_extractor const&)
  {
    return false;
  }
};


/// Extracts the event timestamp.
struct time_extractor : util::totally_ordered<time_extractor>
{
  friend bool operator==(time_extractor const&, time_extractor const&)
  {
    return true;
  }

  friend bool operator<(time_extractor const&, time_extractor const&)
  {
    return false;
  }
};

/// Extracts a specific event type.
struct type_extractor : util::totally_ordered<type_extractor>
{
  type_extractor() = default;

  explicit type_extractor(vast::type t)
    : type{std::move(t)}
  {
  }

  vast::type type;

  friend bool operator==(type_extractor const& lhs, type_extractor const& rhs)
  {
    return lhs.type == rhs.type;
  }

  friend bool operator<(type_extractor const& lhs, type_extractor const& rhs)
  {
    return lhs.type < rhs.type;
  }
};

/// Extracts one or more values.
struct schema_extractor : util::totally_ordered<schema_extractor>
{
  schema_extractor() = default;

  explicit schema_extractor(vast::key k)
    : key{std::move(k)}
  {
  }

  vast::key key;

  friend bool operator==(schema_extractor const& lhs,
                         schema_extractor const& rhs)
  {
    return lhs.key == rhs.key;
  }

  friend bool operator<(schema_extractor const& lhs,
                        schema_extractor const& rhs)
  {
    return lhs.key < rhs.key;
  }
};

/// Extracts a singular value, the "instantiation" of a ::schema_extractor.
struct data_extractor : util::totally_ordered<data_extractor>
{
  data_extractor() = default;

  explicit data_extractor(vast::type t, vast::offset o)
    : type{std::move(t)},
      offset{std::move(o)}
  {
  }

  vast::type type;
  vast::offset offset;

  friend bool operator==(data_extractor const& lhs, data_extractor const& rhs)
  {
    return lhs.type == rhs.type && lhs.offset == rhs.offset;
  }

  friend bool operator<(data_extractor const& lhs, data_extractor const& rhs)
  {
    return std::tie(lhs.type, lhs.offset) < std::tie(rhs.type, rhs.offset);
  }
};

/// A predicate.
struct predicate : util::totally_ordered<predicate>
{
  predicate() = default;

  using operand = util::variant<
      event_extractor,
      time_extractor,
      type_extractor,
      schema_extractor,
      data_extractor,
      data
    >;

  predicate(operand l, relational_operator o, operand r)
    : lhs{std::move(l)},
      op{o},
      rhs{std::move(r)}
  {
  }

  operand lhs;
  relational_operator op;
  operand rhs;

  friend bool operator==(predicate const& lhs, predicate const& rhs)
  {
    return lhs.lhs == rhs.lhs && lhs.op == rhs.op && lhs.rhs == rhs.rhs;
  }

  friend bool operator<(predicate const& lhs, predicate const& rhs)
  {
    return
      std::tie(lhs.lhs, lhs.op, lhs.rhs) < std::tie(rhs.lhs, rhs.op, rhs.rhs);
  }
};

/// A sequence of AND expressions.
struct conjunction : std::vector<expression>
{
  using std::vector<expression>::vector;
};

/// A sequence of OR expressions.
struct disjunction : std::vector<expression>
{
  using std::vector<expression>::vector;
};

/// A NOT expression.
struct negation : std::vector<vast::expression>
{
  using std::vector<vast::expression>::vector;

  vast::expression const& expression() const;
  vast::expression& expression();
};

/// A query expression.
class expression : util::totally_ordered<expression>
{
  friend access;

public:
  using node = util::variant<
    none,
    conjunction,
    disjunction,
    negation,
    predicate
  >;

  /// Default-constructs empty an expression.
  expression(none = nil) {}

  /// Constructs an expression.
  /// @param x The node to construct an expression from.
  template <
    typename T,
    typename U = std::decay_t<T>,
    typename = std::enable_if_t<
         std::is_same<U, none>::value
      || std::is_same<U, conjunction>::value
      || std::is_same<U, disjunction>::value
      || std::is_same<U, negation>::value
      || std::is_same<U, predicate>::value
    >
  >
  expression(T&& x)
    : node_{std::forward<T>(x)}
  {
  }

  friend bool operator==(expression const& lhs, expression const& rhs);
  friend bool operator<(expression const& lhs, expression const& rhs);

  friend node& expose(expression& d);
  friend node const& expose(expression const& d);

private:
  node node_;
};

namespace detail {

template <typename Iterator>
struct expr_printer
{
  expr_printer(Iterator& out)
    : out_{out}
  {
  }

  trial<void> operator()(none) const
  {
    return print("<invalid>", out_);
  }

  trial<void> operator()(conjunction const& c) const
  {
    auto t = print('{', out_);
    if (! t)
      return t;

    t = util::print_delimited(" && ", c.begin(), c.end(), out_);
    if (! t)
      return t;

    return print('}', out_);
  }

  trial<void> operator()(disjunction const& d) const
  {
    auto t = print('(', out_);
    if (! t)
      return t;

    t = util::print_delimited(" || ", d.begin(), d.end(), out_);
    if (! t)
      return t;

    return print(')', out_);
  }

  trial<void> operator()(negation const& n) const
  {
    auto t = print("! ", out_);
    if (! t)
      return t;

    return print(n.expression(), out_);
  }

  trial<void> operator()(predicate const& p) const
  {
    auto t = visit(*this, p.lhs);
    if (! t)
      return t;

    *out_++ = ' ';

    t = print(p.op, out_);
    if (! t)
      return t;

    *out_++ = ' ';

    return visit(*this, p.rhs);
  }

  trial<void> operator()(event_extractor const&) const
  {
    return print("&type", out_);
  }

  trial<void> operator()(time_extractor const&) const
  {
    return print("&time", out_);
  }

  trial<void> operator()(type_extractor const& e) const
  {
    return print(e.type, out_);
  }

  trial<void> operator()(schema_extractor const& e) const
  {
    return print(e.key, out_);
  }

  trial<void> operator()(data_extractor const& e) const
  {
    auto t = print(e.type, out_);
    if (! t)
      return t;

    if (e.offset.empty())
      return nothing;

    t = print('@', out_);
    if (! t)
      return t;

    return print(e.offset, out_);
  }

  trial<void> operator()(data const& d) const
  {
    return print(d, out_);
  }

  Iterator& out_;
};

} // namespace detail

template <typename T, typename Iterator>
auto print(T const& x, Iterator&& out)
  -> std::enable_if_t<
       std::is_same<T, event_extractor>::value
       || std::is_same<T, time_extractor>::value
       || std::is_same<T, type_extractor>::value
       || std::is_same<T, schema_extractor>::value
       || std::is_same<T, data_extractor>::value
       || std::is_same<T, predicate>::value
       || std::is_same<T, conjunction>::value
       || std::is_same<T, disjunction>::value
       || std::is_same<T, negation>::value,
       trial<void>
     >
{
  return detail::expr_printer<Iterator>{out}(x);
}

template <typename Iterator>
trial<void> print(expression const& e, Iterator&& out)
{
  return visit(detail::expr_printer<Iterator>{out}, e);
}

namespace detail {

// Converts a Boost Spirit AST into VAST's internal representation.
class expression_factory
{
public:
  using result_type = trial<expression>;

  trial<expression> operator()(detail::ast::query::query_expr const& q) const
  {
    // Split the query expression at each OR node.
    std::vector<detail::ast::query::query_expr> ors;
    ors.push_back({q.first, {}});
    for (auto& pred : q.rest)
      if (pred.op == logical_or)
        ors.push_back({pred.operand, {}});
      else
        ors.back().rest.push_back(pred);

    // Create a conjunction for each set of subsequent AND nodes between two OR
    // nodes.
    disjunction dis;
    for (auto& ands : ors)
    {
      conjunction con;

      auto t = boost::apply_visitor(expression_factory{}, ands.first);
      if (! t)
        return t;

      con.push_back(std::move(*t));

      for (auto pred : ands.rest)
      {
        auto t = boost::apply_visitor(expression_factory{}, pred.operand);
        if (! t)
          return t;

        con.push_back(std::move(*t));
      }

      dis.emplace_back(std::move(con));
    }

    return expression{std::move(dis)};
  }

  trial<expression> operator()(detail::ast::query::predicate const& p) const
  {
    auto make_extractor = [](std::string const& str)
    -> trial<predicate::operand>
    {
      if (str == "&type")
        return {event_extractor{}};

      if (str == "&time")
        return {time_extractor{}};

      assert(! str.empty());
      if (str[0] == ':')
      {
        // TODO: make vast::type parseable.
        auto s = str.substr(1);
        type t;
        if (s == "bool")
          t = type::boolean{};
        else if (s == "int")
          t = type::integer{};
        else if (s == "count")
          t = type::count{};
        else if (s == "real")
          t = type::real{};
        else if (s == "time")
          t = type::time_point{};
        else if (s == "duration")
          t = type::time_duration{};
        else if (s == "string")
          t = type::string{};
        else if (s == "addr")
          t = type::address{};
        else if (s == "subnet")
          t = type::subnet{};
        else if (s == "port")
          t = type::port{};
        else
          return error{"invalid type: ", s};

        return {type_extractor{std::move(t)}};
      }

      auto t = to<key>(str);
      if (! t)
        return t.error();

      return {schema_extractor{std::move(*t)}};
    };

    auto make_operand = [&](detail::ast::query::predicate::lhs_or_rhs const& lr)
    -> trial<predicate::operand>
    {
      predicate::operand o;
      if (auto d = boost::get<detail::ast::query::data_expr>(&lr))
      {
        o = detail::ast::query::fold(*d);
      }
      else
      {
        auto e = make_extractor(*boost::get<std::string>(&lr));
        if (! e)
          return e;

        o = *e;
      }

      return o;
    };

    auto lhs = make_operand(p.lhs);
    if (! lhs)
      return lhs.error();

    auto rhs = make_operand(p.rhs);
    if (! rhs)
      return rhs.error();

    return expression{predicate{std::move(*lhs), p.op, std::move(*rhs)}};
  }

  trial<expression> operator()(detail::ast::query::negated const& neg) const
  {
    auto t = (*this)(neg.expr);
    if (! t)
      return t.error();

    negation n;
    n.push_back(std::move(*t));

    return expression{std::move(n)};
  }
};

} // namespace detail

template <typename Iterator>
trial<void> parse(expression& e, Iterator& begin, Iterator end)
{
  std::string err;
  detail::parser::error_handler<Iterator> on_error{begin, end, err};
  detail::parser::query<Iterator> grammar{on_error};
  detail::parser::skipper<Iterator> skipper;
  detail::ast::query::query_expr q;

  bool success = phrase_parse(begin, end, grammar, skipper, q);
  if (! success)
    return error{std::move(err)};
  else if (begin != end)
    return error{"input not consumed:'", std::string{begin, end}, "'"};

  auto t = detail::expression_factory{}(q);
  if (! t)
    return t.error();

  e = std::move(*t);

  auto v = visit(expr::validator{}, e);
  if (! v)
    return v.error();

  e = visit(expr::hoister{}, e);

  return nothing;
}

} // namespace vast

#endif
