//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/expression.hpp"

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/legacy_type.hpp"
#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"

namespace vast {

namespace {

using predicate_tuple
  = std::tuple<predicate::operand, relational_operator, predicate::operand>;

static predicate to_predicate(predicate_tuple xs) {
  return {std::move(std::get<0>(xs)), std::get<1>(xs),
          std::move(std::get<2>(xs))};
}

static predicate::operand to_extractor(std::vector<std::string> xs) {
  // TODO: rather than doing all the work with the attributes, it would be nice
  // if the parser framework would just give us an iterator range over the raw
  // input. Then we wouldn't have to use expensive attributes and could simply
  // wrap a parser P into raw(P) or raw_str(P) to obtain a range/string_view.
  auto field = detail::join(xs, ".");
  return extractor{std::move(field)};
}

static predicate::operand to_type_extractor(legacy_type x) {
  return type_extractor{type::from_legacy_type(x)};
}

static predicate::operand to_data_operand(data x) {
  return x;
}

/// Expands a predicate with a type extractor, an equality operator, and a
/// corresponding data instance according to the rules of the expression
/// language.
struct expander {
  expression operator()(caf::none_t) const {
    return expression{};
  }

  expression operator()(const conjunction& c) const {
    conjunction result;
    for (auto& op : c)
      result.push_back(caf::visit(*this, op));
    return result;
  }

  expression operator()(const disjunction& d) const {
    disjunction result;
    for (auto& op : d)
      result.push_back(caf::visit(*this, op));
    return result;
  }

  expression operator()(const negation& n) const {
    return {negation{caf::visit(*this, n.expr())}};
  }

  expression operator()(const predicate& p) const {
    // Builds an additional predicate for subnet type extractor predicates. The
    // additional :addr in S predicate gets appended as disjunction afterwards.
    auto build_addr_pred
      = [](auto& lhs, auto op, auto& rhs) -> caf::optional<expression> {
      if (auto t = caf::get_if<type_extractor>(&lhs))
        if (auto d = caf::get_if<data>(&rhs))
          if (op == relational_operator::equal)
            if (caf::holds_alternative<subnet_type>(t->type))
              if (auto sn = caf::get_if<subnet>(d))
                return predicate{type_extractor{type{address_type{}}},
                                 relational_operator::in, *d};
      return caf::none;
    };
    auto make_disjunction = [](auto x, auto y) {
      disjunction result;
      result.push_back(std::move(x));
      result.push_back(std::move(y));
      return result;
    };
    if (auto addr_pred = build_addr_pred(p.lhs, p.op, p.rhs))
      return make_disjunction(p, std::move(*addr_pred));
    if (auto addr_pred = build_addr_pred(p.rhs, p.op, p.lhs))
      return make_disjunction(p, std::move(*addr_pred));
    return {p};
  }
};

/// Expands a data instance in two steps:
/// 1. Convert the data instance x to T(x) == x
/// 2. Apply type-specific expansion that results in a compound expression
static expression expand(data x) {
  auto infer_type = [](const auto& d) -> type {
    return type::infer(d);
  };
  auto lhs = type_extractor{caf::visit(infer_type, x)};
  auto rhs = predicate::operand{std::move(x)};
  auto pred
    = predicate{std::move(lhs), relational_operator::equal, std::move(rhs)};
  return caf::visit(expander{}, expression{std::move(pred)});
}

static auto make_predicate_parser() {
  using parsers::alnum;
  using parsers::chr;
  using namespace parser_literals;
  // clang-format off
  // TODO: Align this with identifier_char.
  auto field_char = alnum | chr{'_'} | chr{'-'} | chr{':'};
  // A field cannot start with:
  //  - '-' to leave room for potential arithmetic expressions in operands
  //  - ':' so it won't be interpreted as a type extractor
  auto field = !(':'_p | '-') >> (+field_char % '.');
  auto operand
    = (parsers::data >> !(field_char | '.')) ->* to_data_operand
    | "#type"_p  ->* [] { return selector{selector::type}; }
    | "#field"_p ->* [] { return selector{selector::field}; }
    | "#import_time"_p ->* [] { return selector{selector::import_time}; }
    | ':' >> parsers::legacy_type ->* to_type_extractor
    | field ->* to_extractor
    ;
  auto operation
    = "~"_p   ->* [] { return relational_operator::match; }
    | "!~"_p  ->* [] { return relational_operator::not_match; }
    | "=="_p  ->* [] { return relational_operator::equal; }
    | "!="_p  ->* [] { return relational_operator::not_equal; }
    | "<="_p  ->* [] { return relational_operator::less_equal; }
    | "<"_p   ->* [] { return relational_operator::less; }
    | ">="_p  ->* [] { return relational_operator::greater_equal; }
    | ">"_p   ->* [] { return relational_operator::greater; }
    | "in"_p  ->* [] { return relational_operator::in; }
    | "!in"_p ->* [] { return relational_operator::not_in; }
    | "ni"_p  ->* [] { return relational_operator::ni; }
    | "!ni"_p ->* [] { return relational_operator::not_ni; }
    | "[+"_p  ->* [] { return relational_operator::in; }
    | "[-"_p  ->* [] { return relational_operator::not_in; }
    | "+]"_p  ->* [] { return relational_operator::ni; }
    | "-]"_p  ->* [] { return relational_operator::not_ni; }
    ;
  auto ws = ignore(*parsers::space);
  auto pred
    = (operand >> ws >> operation >> ws >> operand) ->* to_predicate
    ;
  return pred;
  // clang-format on
}

} // namespace

template <class Iterator>
bool predicate_parser::parse(Iterator& f, const Iterator& l,
                             unused_type) const {
  static auto p = make_predicate_parser();
  return p(f, l, unused);
}

template <class Iterator>
bool predicate_parser::parse(Iterator& f, const Iterator& l,
                             predicate& a) const {
  using namespace parsers;
  static auto p = make_predicate_parser();
  return p(f, l, a);
}

template bool
predicate_parser::parse(std::string::iterator&, const std::string::iterator&,
                        unused_type) const;
template bool
predicate_parser::parse(std::string::iterator&, const std::string::iterator&,
                        predicate&) const;

template bool
predicate_parser::parse(std::string::const_iterator&,
                        const std::string::const_iterator&, unused_type) const;
template bool
predicate_parser::parse(std::string::const_iterator&,
                        const std::string::const_iterator&, predicate&) const;

template bool
predicate_parser::parse(char const*&, char const* const&, unused_type) const;
template bool
predicate_parser::parse(char const*&, char const* const&, predicate&) const;

namespace {

template <class Iterator>
static auto make_expression_parser() {
  using namespace parser_literals;
  using chain = std::vector<std::tuple<bool_operator, expression>>;
  using raw_expr = std::tuple<expression, chain>;
  // Converts a "raw" chain of sub-expressions and transforms it into an
  // expression tree.
  auto to_expr = [](raw_expr expr) -> expression {
    auto& [x, xs] = expr;
    if (xs.empty())
      return x;
    // We split the expression chain at each OR node in order to take care of
    // operator precedance: AND binds stronger than OR.
    disjunction dis;
    auto con = conjunction{x};
    for (auto& [op, expr] : xs)
      if (op == bool_operator::logical_and) {
        con.emplace_back(std::move(expr));
      } else if (op == bool_operator::logical_or) {
        VAST_ASSERT(!con.empty());
        if (con.size() == 1)
          dis.emplace_back(std::move(con[0]));
        else
          dis.emplace_back(std::move(con));
        con = conjunction{std::move(expr)};
      } else {
        VAST_ASSERT(!"negations must not exist here");
      }
    if (con.size() == 1)
      dis.emplace_back(std::move(con[0]));
    else
      dis.emplace_back(std::move(con));
    return dis.size() == 1 ? std::move(dis[0]) : expression{dis};
  };
  auto ws = ignore(*parsers::space);
  auto negate_expr = [](expression expr) {
    return negation{std::move(expr)};
  };
  rule<Iterator, expression> expr;
  rule<Iterator, expression> group;
  // clang-format off
  auto pred_expr
    = parsers::predicate ->* [](predicate p) { return expression{std::move(p)}; }
    | parsers::data ->* expand;
    ;
  group
    = '(' >> ws >> ref(expr) >> ws >> ')'
    | '!' >> ws >> pred_expr ->* negate_expr
    | '!' >> ws >> '(' >> ws >> (ref(expr) ->* negate_expr) >> ws >> ')'
    | pred_expr
    ;
  auto and_or
    = "||"_p  ->* [] { return bool_operator::logical_or; }
    | "&&"_p  ->* [] { return bool_operator::logical_and; }
    ;
  expr
    // One embedding of the group rule is intentionally not wrapped in a
    // rule_ref, because otherwise the reference count of the internal
    // shared_ptr would go to 0 at end of scope. We don't need this
    // precaution for the expr rule because that is part of the return
    // expression.
    = (group >> *(ws >> and_or >> ws >> ref(group)) >> ws) ->* to_expr
    ;
  // clang-format on
  return expr >> parsers::eoi;
}

} // namespace

template <class Iterator, class Attribute>
bool expression_parser::parse(Iterator& f, const Iterator& l,
                              Attribute& x) const {
  static auto p = make_expression_parser<Iterator>();
  return p(f, l, x);
}

template bool
expression_parser::parse(std::string::iterator&, const std::string::iterator&,
                         unused_type&) const;
template bool
expression_parser::parse(std::string::iterator&, const std::string::iterator&,
                         expression&) const;

template bool expression_parser::parse(std::string::const_iterator&,
                                       const std::string::const_iterator&,
                                       unused_type&) const;
template bool
expression_parser::parse(std::string::const_iterator&,
                         const std::string::const_iterator&, expression&) const;

template bool
expression_parser::parse(char const*&, char const* const&, unused_type&) const;
template bool
expression_parser::parse(char const*&, char const* const&, expression&) const;

} // namespace vast
