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

#ifndef VAST_EXPRESSION_HPP
#define VAST_EXPRESSION_HPP

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "vast/data.hpp"
#include "vast/key.hpp"
#include "vast/offset.hpp"
#include "vast/operator.hpp"
#include "vast/type.hpp"
#include "vast/variant.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"

namespace vast {

class expression;

/// Extracts a specific attributes from an event.
struct attribute_extractor : detail::totally_ordered<attribute_extractor> {
  attribute_extractor(std::string str = {});

  std::string attr;

  friend bool operator==(attribute_extractor const&,
                         attribute_extractor const&);
  friend bool operator<(attribute_extractor const&,
                        attribute_extractor const&);

  template <class Inspector>
  friend auto inspect(Inspector& f, attribute_extractor& ex) {
    return f(ex.attr);
  }
};

/// Extracts one or more values according to a given key.
struct key_extractor : detail::totally_ordered<key_extractor> {
  key_extractor(vast::key k = {});

  vast::key key;

  friend bool operator==(key_extractor const& lhs, key_extractor const& rhs);
  friend bool operator<(key_extractor const& lhs, key_extractor const& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, key_extractor& ex) {
    return f(ex.key);
  }
};

/// Extracts one or more values according to a given key.
struct type_extractor : detail::totally_ordered<type_extractor> {
  type_extractor(vast::type t = {});

  vast::type type;

  friend bool operator==(type_extractor const& lhs, type_extractor const& rhs);
  friend bool operator<(type_extractor const& lhs, type_extractor const& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, type_extractor& ex) {
    return f(ex.type);
  }
};

/// Extracts a specific data value from a type according to an offset. During
/// AST resolution, the ::key_extractor generates multiple instantiations of
/// this extractor according to a given ::schema.
struct data_extractor : detail::totally_ordered<data_extractor> {
  data_extractor() = default;

  data_extractor(vast::type t, vast::offset o);

  vast::type type;
  vast::offset offset;

  friend bool operator==(data_extractor const& lhs, data_extractor const& rhs);
  friend bool operator<(data_extractor const& lhs, data_extractor const& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, data_extractor& ex) {
    return f(ex.type, ex.offset);
  }
};

/// A predicate with two operands evaluated under a relational operator.
struct predicate : detail::totally_ordered<predicate> {
  predicate() = default;

  using operand = variant<
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

  friend bool operator==(predicate const& lhs, predicate const& rhs);
  friend bool operator<(predicate const& lhs, predicate const& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, predicate& p) {
    return f(p.lhs, p.op, p.rhs);
  }
};

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
  negation(expression expr);

  negation(negation const& other);
  negation(negation&& other) noexcept;

  negation& operator=(negation const& other);
  negation& operator=(negation&& other) noexcept;

  // Access the contained expression.
  expression const& expr() const;
  expression& expr();

  friend bool operator==(negation const& lhs, negation const& rhs);
  friend bool operator<(negation const& lhs, negation const& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, negation& n) {
    return f(*n.expr_);
  }

private:
  std::unique_ptr<expression> expr_;
};

/// A query expression.
class expression : detail::totally_ordered<expression> {
  friend access;

public:
  using node = variant<
    none,
    conjunction,
    disjunction,
    negation,
    predicate
  >;

  /// Default-constructs empty an expression.
  expression(none = nil) {
  }

  /// Constructs an expression.
  /// @param x The node to construct an expression from.
  template <
    class T,
    class = std::enable_if_t<detail::contains<std::decay_t<T>, node::types>{}>
  >
  expression(T&& x) : node_{std::forward<T>(x)} {
  }

  friend bool operator==(expression const& lhs, expression const& rhs);
  friend bool operator<(expression const& lhs, expression const& rhs);

  template <class Inspector>
  friend auto inspect(Inspector&f, expression& e) {
    return f(e.node_);
  }

  friend node& expose(expression& d);

private:
  node node_;
};

/// Normalizes an expression such that:
///
/// 1. Single-element conjunctions/disjunctions don't exist.
/// 2. Extractors end up always on the LHS of a predicate.
/// 3. Negations are pushed down to the predicate level.
///
/// @param expr The expression to normalize.
/// @returns The normalized expression.
expression normalize(expression const& expr);

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

#endif
