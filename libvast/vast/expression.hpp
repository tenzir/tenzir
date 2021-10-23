//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/atoms.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/data.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/hash/hash.hpp"
#include "vast/legacy_type.hpp"
#include "vast/offset.hpp"
#include "vast/operator.hpp"

#include <caf/default_sum_type_access.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/none.hpp>
#include <caf/variant.hpp>

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

namespace vast {

class expression;

/// Extracts meta data from an event.
struct meta_extractor : detail::totally_ordered<meta_extractor> {
  enum kind { type, field };

  meta_extractor() = default;

  meta_extractor(kind k) : kind{k} {
    // nop
  }

  kind kind;
};

bool operator==(const meta_extractor& x, const meta_extractor& y);
bool operator<(const meta_extractor& x, const meta_extractor& y);

template <class Inspector>
auto inspect(Inspector& f, meta_extractor& x) {
  return f(caf::meta::type_name("meta_extractor"), x.kind);
}

/// Extracts one or more values according to a given field.
struct field_extractor : detail::totally_ordered<field_extractor> {
  field_extractor(std::string k = {});

  std::string field;
};

/// @relates field_extractor
bool operator==(const field_extractor& x, const field_extractor& y);

/// @relates field_extractor
bool operator<(const field_extractor& x, const field_extractor& y);

/// @relates field_extractor
template <class Inspector>
auto inspect(Inspector& f, field_extractor& x) {
  return f(caf::meta::type_name("field_extractor"), x.field);
}

/// Extracts one or more values according to a given type.
struct type_extractor : detail::totally_ordered<type_extractor> {
  type_extractor(vast::legacy_type t = {});

  vast::legacy_type type;
};

/// @relates type_extractor
bool operator==(const type_extractor& x, const type_extractor& y);

/// @relates type_extractor
bool operator<(const type_extractor& x, const type_extractor& y);

/// @relates type_extractor
template <class Inspector>
auto inspect(Inspector& f, type_extractor& x) {
  return f(caf::meta::type_name("type_extractor"), x.type);
}

/// Extracts a specific data value from a type according to an offset. During
/// AST resolution, the ::field_extractor generates multiple instantiations of
/// this extractor for a given ::schema.
struct data_extractor : detail::totally_ordered<data_extractor> {
  data_extractor() = default;

  data_extractor(vast::legacy_type t, vast::offset o);

  vast::legacy_type type;
  vast::offset offset;
};

/// @relates data_extractor
bool operator==(const data_extractor& x, const data_extractor& y);

/// @relates data_extractor
bool operator<(const data_extractor& x, const data_extractor& y);

/// @relates data_extractor
template <class Inspector>
auto inspect(Inspector& f, data_extractor& x) {
  return f(caf::meta::type_name("data_extractor"), x.type, x.offset);
}

/// A predicate with two operands evaluated under a relational operator.
struct predicate : detail::totally_ordered<predicate> {
  /// The operand of a predicate, which can be either LHS or RHS.
  using operand = caf::variant<meta_extractor, field_extractor, type_extractor,
                               data_extractor, data>;

  predicate() = default;

  predicate(operand l, relational_operator o, operand r);

  operand lhs;
  relational_operator op;
  operand rhs;
};

/// @relates predicate
bool operator==(const predicate& x, const predicate& y);

/// @relates predicate
bool operator<(const predicate& x, const predicate& y);

/// @relates predicate
template <class Inspector>
auto inspect(Inspector& f, predicate& x) {
  return f(caf::meta::type_name("predicate"), x.lhs, x.op, x.rhs);
}

/// A curried predicate, i.e., a predicate with its `lhs` operand fixed by an
/// outer scope or context.
struct curried_predicate {
  using operand = predicate::operand;

  relational_operator op;
  data rhs;
};

/// @relates curried_predicate
template <class Inspector>
auto inspect(Inspector& f, curried_predicate& x) {
  return f(caf::meta::type_name("curried_predicate"), x.op, x.rhs);
}

/// @returns a curried version of `pred`.
/// @relates predicate
/// @relates curried_predicate
curried_predicate curried(const predicate& pred);

/// A sequence of AND expressions.
struct conjunction : std::vector<expression> {
  using super = std::vector<expression>;
  using super::vector;
  explicit conjunction(const super& other);
  explicit conjunction(super&& other) noexcept;
};

/// @relates conjunction
template <class Inspector>
auto inspect(Inspector& f, conjunction& x) {
  return f(caf::meta::type_name("conjunction"),
           static_cast<std::vector<expression>&>(x));
}

/// A sequence of OR expressions.
struct disjunction : std::vector<expression> {
  using super = std::vector<expression>;
  using super::vector;
  explicit disjunction(const super& other);
  explicit disjunction(super&& other) noexcept;
};

/// @relates conjunction
template <class Inspector>
auto inspect(Inspector& f, disjunction& x) {
  return f(caf::meta::type_name("disjunction"),
           static_cast<std::vector<expression>&>(x));
}

/// A NOT expression.
struct negation : detail::totally_ordered<negation> {
  negation();
  explicit negation(expression expr);

  negation(const negation& other);
  negation(negation&& other) noexcept;

  negation& operator=(const negation& other);
  negation& operator=(negation&& other) noexcept;

  // Access the contained expression.
  [[nodiscard]] const expression& expr() const;
  expression& expr();

private:
  std::unique_ptr<expression> expr_;
};

/// @relates negation
bool operator==(const negation& x, const negation& y);

/// @relates negation
bool operator<(const negation& x, const negation& y);

/// @relates negation
template <class Inspector>
auto inspect(Inspector& f, negation& x) {
  return f(caf::meta::type_name("negation"), x.expr());
}

/// A query expression.
class expression : detail::totally_ordered<expression> {
public:
  using types = caf::detail::type_list<caf::none_t, conjunction, disjunction,
                                       negation, predicate>;

  using node = caf::detail::tl_apply_t<types, caf::variant>;

  /// Default-constructs empty an expression.
  expression() = default;

  /// Constructs an expression.
  /// @param x The node to construct an expression from.
  template <class T>
    requires(detail::contains_type_v<types, std::decay_t<T>>)
  expression(T&& x) : node_(std::forward<T>(x)) {
    // nop
  }

  /// @cond PRIVATE

  [[nodiscard]] const node& get_data() const;
  node& get_data();

  /// @endcond

private:
  node node_;
};

/// @relates expression
bool operator==(const expression& x, const expression& y);

/// @relates expression
bool operator<(const expression& x, const expression& y);

/// @relates expression
template <class Inspector>
auto inspect(Inspector& f, expression& x) {
  return f(caf::meta::type_name("expression"), x.get_data());
}

template <class F>
struct predicate_transformer {
  using result_type = std::invoke_result_t<F, const predicate&>;

  result_type operator()(caf::none_t) const {
    return expression{caf::none};
  }

  result_type operator()(const conjunction& c) const {
    return make_result(c);
  }

  result_type operator()(const disjunction& c) const {
    return make_result(c);
  }

  result_type operator()(const negation& n) const {
    auto x = caf::visit(*this, n.expr());
    if constexpr (std::is_convertible_v<result_type, expression>) {
      return {negation{std::move(x)}};
    } else {
      return {negation{std::move(*x)}};
    }
  }

  result_type operator()(const predicate& p) const {
    return f(p);
  }

  F f;

private:
  template <class T>
  result_type make_result(const T& input) const {
    T result;
    for (const auto& op : input) {
      auto x = caf::visit(*this, op);
      if constexpr (std::is_convertible_v<result_type, typename T::value_type>) {
        result.push_back(std::move(x));
      } else {
        if (!x)
          return x;
        else
          result.push_back(std::move(*x));
      }
    }
    return result;
  }
};

/// Applies a transformation for every predicate in an expression.
/// @param expr The input expression.
/// @param f A callable that takes a predicate and returns an expression.
/// @returns The transformed expression.
template <typename F>
auto for_each_predicate(const expression& e, F&& f) {
  auto v = predicate_transformer<F>{std::forward<F>(f)};
  return caf::visit(v, e);
}

/// Transforms an expression by pulling out nested connectives with a single
/// operand into the top-level connective. For example, (x == 1 || (x == 2))
/// becomes (x == 1 || x == 2).
/// @param expr The expression to hoist.
/// @returns The hoisted expression.
expression hoist(expression expr);

/// Removes predicates with meta extractors from the tree.
/// @param expr The expression to prune.
/// @returns The pruned expression.
expression prune_meta_predicates(expression expr);

/// Normalizes an expression such that:
///
/// 1. Single-element conjunctions/disjunctions don't exist.
/// 2. Extractors end up always on the LHS of a predicate.
/// 3. Negations are pushed down to the predicate level.
///
/// @param expr The expression to normalize.
/// @returns The normalized expression.
expression normalize(expression expr);

/// [Normalizes](@ref normalize) and [validates](@ref validator) an expression.
/// @param expr The expression to normalize and validate.
/// @returns The normalized and validated expression on success.
caf::expected<expression> normalize_and_validate(expression expr);

/// Tailors an expression to a specific type.
/// @param expr The expression to tailor to *t*.
/// @param t The type to tailor *expr* to.
/// @returns An optimized version of *expr* specifically for evaluating events
///          of type *t*.
caf::expected<expression> tailor(expression expr, const legacy_type& t);

/// Retrieves an expression node at a given [offset](@ref offset).
/// @param expr The expression to lookup.
/// @param o The offset corresponding to a node in *expr*.
/// @returns The expression node at *o* or `nullptr` if *o* does not describe a
///          valid offset for *expr*.
const expression* at(const expression& expr, const offset& o);

/// Resolves expression predicates according to given type. The resolution
/// includes replacement of field and type extractors with data extractors
/// pertaining to the given type.
/// @param expr The expression whose predicates to resolve.
/// @param t The type according to which extractors should be resolved.
/// @returns A mapping of offsets to replaced predicates. Each offset uniquely
///          identifies a predicate in *expr* and the mapped values represent
///          the new predicates.
std::vector<std::pair<offset, predicate>>
resolve(const expression& expr, const legacy_type& t);

} // namespace vast

namespace caf {

template <>
struct sum_type_access<vast::expression>
  : default_sum_type_access<vast::expression> {
  // nop
};

} // namespace caf

namespace std {

template <>
struct hash<vast::meta_extractor> {
  size_t operator()(const vast::meta_extractor& x) const {
    return vast::hash(x);
  }
};

template <>
struct hash<vast::field_extractor> {
  size_t operator()(const vast::field_extractor& x) const {
    return vast::hash(x);
  }
};

template <>
struct hash<vast::type_extractor> {
  size_t operator()(const vast::type_extractor& x) const {
    return vast::hash(x);
  }
};

template <>
struct hash<vast::data_extractor> {
  size_t operator()(const vast::data_extractor& x) const {
    return vast::hash(x);
  }
};

template <>
struct hash<vast::predicate> {
  size_t operator()(const vast::predicate& x) const {
    return vast::hash(x);
  }
};

template <>
struct hash<vast::expression> {
  size_t operator()(const vast::expression& x) const {
    return vast::hash(x);
  }
};

} // namespace std

#include "vast/concept/printable/vast/expression.hpp"

namespace fmt {

template <>
struct formatter<vast::expression> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::expression& value, FormatContext& ctx) {
    auto out = ctx.out();
    vast::print(out, value);
    return out;
  }
};

template <>
struct formatter<vast::data_extractor> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::data_extractor& value, FormatContext& ctx) {
    auto out = ctx.out();
    vast::print(out, value);
    return out;
  }
};

template <>
struct formatter<vast::meta_extractor> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::meta_extractor& value, FormatContext& ctx) {
    auto out = ctx.out();
    vast::print(out, value);
    return out;
  }
};

template <>
struct formatter<vast::relational_operator> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::relational_operator& value, FormatContext& ctx) {
    auto out = ctx.out();
    vast::print(out, value);
    return out;
  }
};

template <>
struct formatter<vast::predicate> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::predicate& value, FormatContext& ctx) {
    auto out = ctx.out();
    vast::print(out, value);
    return out;
  }
};

template <>
struct formatter<vast::curried_predicate> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::curried_predicate& value, FormatContext& ctx) {
    return format_to(ctx.out(), "{} {}", value.op, value.rhs);
  }
};

} // namespace fmt
