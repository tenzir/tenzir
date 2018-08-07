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

#include <caf/default_sum_type_access.hpp>
#include <caf/detail/type_list.hpp>
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
  attribute_extractor(std::string str = {});

  std::string attr;
};

bool operator==(const attribute_extractor& x, const attribute_extractor& y);
bool operator<(const attribute_extractor& x, const attribute_extractor& y);

template <class Inspector>
auto inspect(Inspector& f, attribute_extractor& x) {
  return f(x.attr);
}

/// Extracts one or more values according to a given key.
struct key_extractor : detail::totally_ordered<key_extractor> {
  key_extractor(std::string k = {});

  std::string key;
};

/// @relates key_extractor
bool operator==(const key_extractor& x, const key_extractor& y);

/// @relates key_extractor
bool operator<(const key_extractor& x, const key_extractor& y);

/// @relates key_extractor
template <class Inspector>
auto inspect(Inspector& f, key_extractor& x) {
  return f(x.key);
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
  return f(x.type);
}

/// Extracts a specific data value from a type according to an offset. During
/// AST resolution, the ::key_extractor generates multiple instantiations of
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
  return f(x.type, x.offset);
}

/// A predicate with two operands evaluated under a relational operator.
struct predicate : detail::totally_ordered<predicate> {
  predicate() = default;

  /// The operand of a predicate, which can be either LHS or RHS.
  using operand = caf::variant<
      attribute_extractor,
      key_extractor,
      type_extractor,
      data_extractor,
      data
    >;

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
  return f(x.lhs, x.op, x.rhs);
}

/// A sequence of AND expressions.
struct conjunction : std::vector<expression> {
  using std::vector<expression>::vector;
};

/// A sequence of OR expressions.
struct disjunction : std::vector<expression> {
  using std::vector<expression>::vector;
};

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
  return f(x.expr());
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
  return f(x.get_data());
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
expected<expression> normalize_and_validate(const expression& expr);

/// Tailors an expression to a specific type.
/// @param expr The expression to tailor to *t*.
/// @param t The type to tailor *expr* to.
/// @returns An optimized version of *expr* specifically for evaluating events
///          of type *t*.
expected<expression> tailor(const expression& expr, const type& t);

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

template<>
struct hash<vast::key_extractor> {
  size_t operator()(const vast::key_extractor& x) const {
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
