#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_EXPRESSION_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_EXPRESSION_H

#include "vast/expression.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/vast/detail/query_ast.h"
#include "vast/concept/parseable/vast/detail/error_handler.h"
#include "vast/concept/parseable/vast/detail/skipper.h"
#include "vast/concept/parseable/vast/detail/query.h"
#include "vast/concept/parseable/vast/key.h"

namespace vast {
namespace detail {

// Converts a Boost Spirit AST into VAST's internal representation.
struct expression_factory
{
  using result_type = trial<expression>;

  trial<expression> operator()(ast::query::query_expr const& q) const
  {
    // Split the query expression at each OR node.
    std::vector<ast::query::query_expr> ors;
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

  trial<expression> operator()(ast::query::predicate const& p) const
  {
    auto make_extractor = [](std::string const& str)
    -> trial<predicate::operand>
    {
      if (str == "&type")
        return {event_extractor{}};
      if (str == "&time")
        return {time_extractor{}};
      VAST_ASSERT(! str.empty());
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
        return error{"failed to parse key"};
      return {schema_extractor{std::move(*t)}};
    };

    auto make_operand = [&](ast::query::predicate::lhs_or_rhs const& lr)
      -> trial<predicate::operand>
    {
      predicate::operand o;
      if (auto d = boost::get<ast::query::data_expr>(&lr))
      {
        o = ast::query::fold(*d);
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

  trial<expression> operator()(ast::query::negated const& neg) const
  {
    auto t = (*this)(neg.expr);
    if (! t)
      return t.error();
    negation n;
    n.push_back(std::move(*t));
    return expression{std::move(n)};
  }
};

// Legacy parser until we roll our own.
struct expression_parser : vast::parser<expression_parser>
{
  using attribute = expression;

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, expression& a) const
  {
    std::string err;
    detail::parser::error_handler<Iterator> on_error{f, l, err};
    detail::parser::query<Iterator> grammar{on_error};
    detail::parser::skipper<Iterator> skipper;
    ast::query::query_expr q;
    if (! phrase_parse(f, l, grammar, skipper, q))
      return false;
    auto t = expression_factory{}(q);
    if (! t)
      return false;
    a = std::move(*t);
    auto v = visit(expr::validator{}, a);
    if (! v)
      return false;
    a = visit(expr::hoister{}, a);
    return true;
  }
};

} // namespace detail
} // namespace vast

#endif
