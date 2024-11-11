//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/expression.hpp"

#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/parseable/tenzir/identifier.hpp"
#include "tenzir/concept/parseable/tenzir/legacy_type.hpp"
#include "tenzir/concept/parseable/tenzir/pipeline.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/operator.hpp"

namespace tenzir {

namespace {

using predicate_tuple = std::tuple<operand, relational_operator, operand>;

static predicate to_predicate(predicate_tuple xs) {
  return {std::move(std::get<0>(xs)), std::get<1>(xs),
          std::move(std::get<2>(xs))};
}

static operand to_field_extractor(std::vector<std::string> xs) {
  // TODO: rather than doing all the work with the attributes, it would be nice
  // if the parser framework would just give us an iterator range over the raw
  // input. Then we wouldn't have to use expensive attributes and could simply
  // wrap a parser P into raw(P) or raw_str(P) to obtain a range/string_view.
  auto field = detail::join(xs, ".");
  return field_extractor{std::move(field)};
}

static operand to_type_extractor(legacy_type x) {
  return type_extractor{type::from_legacy_type(x)};
}

static operand to_data_operand(data x) {
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
    // additional :ip in S predicate gets appended as disjunction afterwards.
    auto build_addr_pred
      = [](auto& lhs, auto op, auto& rhs) -> caf::optional<expression> {
      if (auto t = try_as<type_extractor>(&lhs)) {
        if (auto d = try_as<data>(&rhs)) {
          if (op == relational_operator::equal) {
            if (caf::holds_alternative<subnet_type>(t->type)) {
              if (auto sn = try_as<subnet>(d)) {
                return predicate{type_extractor{type{ip_type{}}},
                                 relational_operator::in, *d};
              }
            }
          }
        }
      }
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
  // If the type is a numeric type we do a special expansion that binds to all
  // three numeric types. It's unfortunate to have this much logic during
  // parsing, but this is where we currently expand value predicates so we can
  // only do this here.
  auto try_expand_numeric_literal = []<class T>(const T& lit) -> expression {
    auto result = disjunction{};
    if constexpr (std::is_same_v<T, int64_t>) {
      result.push_back(predicate{type_extractor{int64_type{}},
                                 relational_operator::equal, data{lit}});
      if (lit >= 0) {
        result.push_back(predicate{type_extractor{uint64_type{}},
                                   relational_operator::equal,
                                   data{static_cast<uint64_t>(lit)}});
      }
      result.push_back(predicate{type_extractor{double_type{}},
                                 relational_operator::equal,
                                 data{static_cast<double>(lit)}});
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      if (lit <= std::numeric_limits<int64_t>::max()) {
        result.push_back(predicate{type_extractor{int64_type{}},
                                   relational_operator::equal,
                                   data{static_cast<int64_t>(lit)}});
      }
      result.push_back(predicate{type_extractor{uint64_type{}},
                                 relational_operator::equal, data{lit}});
      result.push_back(predicate{type_extractor{double_type{}},
                                 relational_operator::equal,
                                 data{static_cast<double>(lit)}});
    } else if constexpr (std::is_same_v<T, double>) {
      result.push_back(predicate{type_extractor{int64_type{}},
                                 relational_operator::equal,
                                 data{static_cast<int64_t>(lit)}});
      result.push_back(predicate{type_extractor{uint64_type{}},
                                 relational_operator::equal,
                                 data{static_cast<uint64_t>(lit)}});
      result.push_back(predicate{type_extractor{double_type{}},
                                 relational_operator::equal, data{lit}});
    }
    return result.empty() ? expression{} : expression{std::move(result)};
  };
  if (auto expr = caf::visit(try_expand_numeric_literal, x);
      expr != expression{}) {
    return expr;
  }
  auto infer_type = [](const auto& d) -> type {
    return type::infer(d).value_or(type{});
  };
  auto lhs = type_extractor{caf::visit(infer_type, x)};
  auto rhs = operand{std::move(x)};
  auto pred
    = predicate{std::move(lhs), relational_operator::equal, std::move(rhs)};
  return caf::visit(expander{}, expression{std::move(pred)});
}

expression expand_extractor(operand lhs) {
  return predicate{
    std::move(lhs),
    relational_operator::not_equal,
    operand{data{}},
  };
}

auto make_field_char_parser() {
  using parsers::chr;
  // TODO: Align this with identifier_char.
  return parsers::alnum | chr{'_'} | chr{'-'} | chr{':'};
}

/// Creates a parser that returns an operand containing either a type or field
/// extractor.
auto make_extractor_parser() {
  using namespace parser_literals;
  // A field cannot start with:
  //  - '-' to leave room for potential arithmetic expressions in operands
  //  - ':' so it won't be interpreted as a type extractor
  auto field = !(':'_p | '-') >> (+make_field_char_parser() % '.');
  // clang-format off
  auto extractor
    = ':' >> parsers::legacy_type ->* to_type_extractor
    | field ->* to_field_extractor;
  // clang-format on
  return extractor;
}

/// Creates a parser that returns an operand.
auto make_operand_parser() {
  using namespace parser_literals;
  // clang-format off
  return (parsers::data >> !(make_field_char_parser() | '.')) ->* to_data_operand
    | "#schema_id"_p  ->* [] { return meta_extractor{meta_extractor::schema_id}; }
    | "#schema"_p  ->* [] { return meta_extractor{meta_extractor::schema}; }
    | "#import_time"_p ->* [] { return meta_extractor{meta_extractor::import_time}; }
    | make_extractor_parser();
  // clang-format on
}

auto make_predicate_parser() {
  using parsers::alnum;
  using parsers::chr;
  using parsers::identifier;
  using parsers::optional_ws_or_comment;
  using parsers::required_ws_or_comment;
  using namespace parser_literals;
  // clang-format off
  auto operand = make_operand_parser();
  auto operation
    = "=="_p  ->* [] { return relational_operator::equal; }
    | "!="_p  ->* [] { return relational_operator::not_equal; }
    | "<="_p  ->* [] { return relational_operator::less_equal; }
    | "<"_p   ->* [] { return relational_operator::less; }
    | ">="_p  ->* [] { return relational_operator::greater_equal; }
    | ">"_p   ->* [] { return relational_operator::greater; }
    | "in"_p  ->* [] { return relational_operator::in; }
    | "!in"_p ->* [] { return relational_operator::not_in; }
    | "ni"_p  ->* [] { return relational_operator::ni; }
    | "!ni"_p ->* [] { return relational_operator::not_ni; }
    | ("not"_p >> !&identifier) >> required_ws_or_comment >> "in"_p
      ->* [] { return relational_operator::not_in; }
    ;
  auto pred
    = (operand >> optional_ws_or_comment >> operation >> optional_ws_or_comment >> operand) ->* to_predicate
    ;
  return pred;
  // clang-format on
}

template <class Iterator>
static auto make_expression_parser() {
  using namespace parser_literals;
  using parsers::identifier;
  using parsers::optional_ws_or_comment;
  using chain = std::vector<std::tuple<bool_operator, expression>>;
  using raw_expr = std::tuple<expression, chain>;
  // Converts a "raw" chain of sub-expressions and pipelines it into an
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
        TENZIR_ASSERT(!con.empty());
        if (con.size() == 1)
          dis.emplace_back(std::move(con[0]));
        else
          dis.emplace_back(std::move(con));
        con = conjunction{std::move(expr)};
      } else {
        TENZIR_ASSERT(!"negations must not exist here");
      }
    if (con.size() == 1)
      dis.emplace_back(std::move(con[0]));
    else
      dis.emplace_back(std::move(con));
    return dis.size() == 1 ? std::move(dis[0]) : expression{dis};
  };
  auto negate_expr = [](expression expr) {
    return negation{std::move(expr)};
  };
  rule<Iterator, expression> expr;
  rule<Iterator, expression> group;
  // clang-format off
  auto pred_expr
    = parsers::predicate ->* [](predicate p) { return expression{std::move(p)}; }
    | parsers::data ->* expand
    | make_extractor_parser() ->* expand_extractor
    ;
  group
    = '(' >> optional_ws_or_comment >> ref(expr) >> optional_ws_or_comment >> ')'
    | ('!'_p | ("not"_p >> !&identifier)) >> optional_ws_or_comment >> pred_expr ->* negate_expr
    | ('!'_p |  ("not"_p >> !&identifier)) >> optional_ws_or_comment >> '(' >> optional_ws_or_comment >> (ref(expr) ->* negate_expr) >> optional_ws_or_comment >> ')'
    | pred_expr
    ;
  auto and_or
    = ("||"_p  | "or" )->* [] { return bool_operator::logical_or; }
    | ("&&"_p  | "and" )->* [] { return bool_operator::logical_and; }
    ;
  expr
    // One embedding of the group rule is intentionally not wrapped in a
    // rule_ref, because otherwise the reference count of the internal
    // shared_ptr would go to 0 at end of scope. We don't need this
    // precaution for the expr rule because that is part of the return
    // expression.
    = (group >> *(optional_ws_or_comment >> and_or >> optional_ws_or_comment >> ref(group)) >> optional_ws_or_comment) ->* to_expr
    ;
  // clang-format on
  return expr;
}

} // namespace

template <class Iterator>
bool operand_parser::parse(Iterator& f, const Iterator& l, unused_type) const {
  static auto p = make_operand_parser();
  return p(f, l, unused);
}

template <class Iterator>
bool operand_parser::parse(Iterator& f, const Iterator& l, attribute& a) const {
  using namespace parsers;
  static auto p = make_operand_parser();
  return p(f, l, a);
}

template <class Iterator>
bool predicate_parser::parse(Iterator& f, const Iterator& l,
                             unused_type) const {
  static auto p = make_predicate_parser();
  return p(f, l, unused);
}

template <class Iterator>
bool predicate_parser::parse(Iterator& f, const Iterator& l,
                             attribute& a) const {
  using namespace parsers;
  static auto p = make_predicate_parser();
  return p(f, l, a);
}

template <class Iterator, class Attribute>
bool expression_parser::parse(Iterator& f, const Iterator& l,
                              Attribute& x) const {
  static auto p = make_expression_parser<Iterator>();
  return p(f, l, x);
}

// WARNING: Here be ~~dragons~~ manual template instantiations. This parser is
// included in way too many places, so this is a compile-time optimization.

template bool
operand_parser::parse(std::string::iterator&, const std::string::iterator&,
                      unused_type) const;
template bool
operand_parser::parse(std::string::iterator&, const std::string::iterator&,
                      operand&) const;

template bool
operand_parser::parse(std::string::const_iterator&,
                      const std::string::const_iterator&, unused_type) const;
template bool
operand_parser::parse(std::string::const_iterator&,
                      const std::string::const_iterator&, operand&) const;

template bool
operand_parser::parse(char const*&, char const* const&, unused_type) const;
template bool
operand_parser::parse(char const*&, char const* const&, operand&) const;

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

} // namespace tenzir
