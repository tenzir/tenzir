/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <caf/atom.hpp>
#include <caf/default_sum_type_access.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/none.hpp>
#include <caf/variant.hpp>

#include "vast/data.hpp"
#include "vast/offset.hpp"
#include "vast/operator.hpp"
#include "vast/type.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"

#include "vast/detail/operators.hpp"

namespace vast {

class expression;

/// Extracts a specific attributes from an event.
struct attribute_extractor : detail::totally_ordered<attribute_extractor> {
  attribute_extractor(caf::atom_value str = caf::atom(""));

  caf::atom_value attr;
};

bool operator==(const attribute_extractor& x, const attribute_extractor& y);
bool operator<(const attribute_extractor& x, const attribute_extractor& y);

template <class Inspector>
auto inspect(Inspector& f, attribute_extractor& x) {
  return f(caf::meta::type_name("attribute_extractor"), x.attr);
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
  type_extractor(vast::type t = {});

  vast::type type;

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

  data_extractor(vast::type t, vast::offset o);

  vast::type type;
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
  predicate() = default;

  /// The operand of a predicate, which can be either LHS or RHS.
  using operand = caf::variant<attribute_extractor, field_extractor,
                               type_extractor, data_extractor, data>;

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
  const expression& expr() const;
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
  using types = caf::detail::type_list<
    caf::none_t,
    conjunction,
    disjunction,
    negation,
    predicate
  >;

  using node = caf::detail::tl_apply_t<types, caf::variant>;

  /// Default-constructs empty an expression.
  expression() = default;

  /// Constructs an expression.
  /// @param x The node to construct an expression from.
  template <
    class T,
    class = std::enable_if_t<
      caf::detail::tl_contains<types, std::decay_t<T>>::value
    >
  >
  expression(T&& x) : node_(std::forward<T>(x)) {
    // nop
  }

  /// @cond PRIVATE

  const node& get_data() const;
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
auto inspect(Inspector&f, expression& x) {
  return f(caf::meta::type_name("expression"), x.get_data());
}

template <class F>
struct predicate_transformer {
  using result_type = std::invoke_result_t<F, const predicate&>;

  result_type operator()(caf::none_t) const {
    return expression{caf::none};
  }

  result_type operator()(const conjunction& c) const {
    conjunction result;
    for (auto& op : c) {
      auto x = caf::visit(*this, op);
      if constexpr (std::is_convertible_v<result_type, conjunction::value_type>) {
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

  result_type operator()(const disjunction& c) const {
    disjunction result;
    for (auto& op : c) {
      auto x = caf::visit(*this, op);
      if constexpr (std::is_convertible_v<result_type, disjunction::value_type>) {
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

/// Normalizes an expression such that:
///
/// 1. Single-element conjunctions/disjunctions don't exist.
/// 2. Extractors end up always on the LHS of a predicate.
/// 3. Negations are pushed down to the predicate level.
///
/// @param expr The expression to normalize.
/// @returns The normalized expression.
expression normalize(const expression& expr);

/// [Normalizes](@ref normalize) and [validates](@ref validator) an expression.
/// @param expr The expression to normalize and validate.
/// @returns The normalized and validated expression on success.
caf::expected<expression> normalize_and_validate(const expression& expr);

/// Tailors an expression to a specific type.
/// @param expr The expression to tailor to *t*.
/// @param t The type to tailor *expr* to.
/// @returns An optimized version of *expr* specifically for evaluating events
///          of type *t*.
caf::expected<expression> tailor(const expression& expr, const type& t);

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
resolve(const expression& expr, const type& t);

} // namespace vast

namespace caf {

template <>
struct sum_type_access<vast::expression>
  : default_sum_type_access<vast::expression> {
  // nop
};

} // namespace caf

namespace std {

template<>
struct hash<vast::attribute_extractor> {
  size_t operator()(const vast::attribute_extractor& x) const {
    return vast::uhash<vast::xxhash>{}(x);
  }
};

template <>
struct hash<vast::field_extractor> {
  size_t operator()(const vast::field_extractor& x) const {
    return vast::uhash<vast::xxhash>{}(x);
  }
};

template<>
struct hash<vast::type_extractor> {
  size_t operator()(const vast::type_extractor& x) const {
    return vast::uhash<vast::xxhash>{}(x);
  }
};

template<>
struct hash<vast::data_extractor> {
  size_t operator()(const vast::data_extractor& x) const {
    return vast::uhash<vast::xxhash>{}(x);
  }
};

template<>
struct hash<vast::predicate> {
  size_t operator()(const vast::predicate& x) const {
    return vast::uhash<vast::xxhash>{}(x);
  }
};

template<>
struct hash<vast::expression> {
  size_t operator()(const vast::expression& x) const {
    return vast::uhash<vast::xxhash>{}(x);
  }
};

} // namespace std
