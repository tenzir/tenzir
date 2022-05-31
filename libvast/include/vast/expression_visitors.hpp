//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/offset.hpp"
#include "vast/operator.hpp"
#include "vast/time.hpp"

#include <caf/expected.hpp>

#include <vector>

namespace vast {

/// Hoists the contained expression of a single-element conjunction or
/// disjunction one level in the tree.
struct hoister {
  expression operator()(caf::none_t) const;
  expression operator()(const conjunction& c) const;
  expression operator()(const disjunction& d) const;
  expression operator()(const negation& n) const;
  expression operator()(const predicate& p) const;
};

/// Removes meta predicates from the tree.
struct meta_pruner {
  expression operator()(caf::none_t) const;
  expression operator()(const conjunction& c) const;
  expression operator()(const disjunction& d) const;
  expression operator()(const negation& n) const;
  expression operator()(const predicate& p) const;
};

/// Ensures that extractors always end up on the LHS of a predicate.
struct aligner {
  expression operator()(caf::none_t) const;
  expression operator()(const conjunction& c) const;
  expression operator()(const disjunction& d) const;
  expression operator()(const negation& n) const;
  expression operator()(const predicate& p) const;
};

/// Pushes negations down to the predicate level and removes double negations.
struct denegator {
  explicit denegator(bool negate = false);

  expression operator()(caf::none_t) const;
  expression operator()(const conjunction& c) const;
  expression operator()(const disjunction& d) const;
  expression operator()(const negation& n) const;
  expression operator()(const predicate& p) const;

  bool negate_ = false;
};

/// Removes duplicate predicates from conjunctions and disjunctions.
struct deduplicator {
  expression operator()(caf::none_t) const;
  expression operator()(const conjunction& c) const;
  expression operator()(const disjunction& d) const;
  expression operator()(const negation& n) const;
  expression operator()(const predicate& p) const;
};

/// Extracts all predicates from an expression.
struct predicatizer {
  std::vector<predicate> operator()(caf::none_t) const;
  std::vector<predicate> operator()(const conjunction& c) const;
  std::vector<predicate> operator()(const disjunction& d) const;
  std::vector<predicate> operator()(const negation& n) const;
  std::vector<predicate> operator()(const predicate& p) const;
};

/// Ensures that LHS and RHS of a predicate fit together.
struct validator {
  caf::expected<void> operator()(caf::none_t);
  caf::expected<void> operator()(const conjunction& c);
  caf::expected<void> operator()(const disjunction& d);
  caf::expected<void> operator()(const negation& n);
  caf::expected<void> operator()(const predicate& p);
  caf::expected<void> operator()(const selector& ex, const data& d);
  caf::expected<void> operator()(const type_extractor& ex, const data& d);
  caf::expected<void> operator()(const extractor& ex, const data& d);

  template <class T, class U>
  caf::expected<void> operator()(const T& lhs, const U& rhs) {
    return caf::make_error(ec::syntax_error, "incompatible predicate operands",
                           lhs, rhs);
  }

  relational_operator op_;
};

/// Transforms all ::extractor and predicates into ::data_extractor instances
/// according to a given type.
struct type_resolver {
  explicit type_resolver(const type& layout);

  caf::expected<expression> operator()(caf::none_t);
  caf::expected<expression> operator()(const conjunction& c);
  caf::expected<expression> operator()(const disjunction& d);
  caf::expected<expression> operator()(const negation& n);
  caf::expected<expression> operator()(const predicate& p);
  caf::expected<expression> operator()(const selector& ex, const data& d);
  caf::expected<expression> operator()(const type_extractor& ex, const data& d);
  caf::expected<expression> operator()(const data& d, const type_extractor& ex);
  caf::expected<expression> operator()(const extractor& ex, const data& d);
  caf::expected<expression> operator()(const data& d, const extractor& e);

  template <class T, class U>
  caf::expected<expression> operator()(const T& lhs, const U& rhs) {
    return {predicate{lhs, op_, rhs}};
  }

  // Attempts to resolve all extractors that fulfil a given property.
  // provided property is a function that returns an error
  template <class Function>
  [[nodiscard]] expression resolve_extractor(Function f, const data& x) const {
    std::vector<expression> connective;
    auto make_predicate = [&](type t, size_t i) {
      return predicate{data_extractor{std::move(t), i}, op_, x};
    };
    for (size_t flat_index = 0; const auto& [field, _] : layout_.leaves()) {
      if (f(field.type))
        connective.emplace_back(make_predicate(field.type, flat_index));
      ++flat_index;
    }
    if (connective.empty())
      return expression{}; // did not resolve
    if (connective.size() == 1)
      return {std::move(connective[0])}; // hoist expression
    if (op_ == relational_operator::not_equal
        || op_ == relational_operator::not_match
        || op_ == relational_operator::not_in
        || op_ == relational_operator::not_ni)
      return {conjunction{std::move(connective)}};
    else
      return {disjunction{std::move(connective)}};
  }

  relational_operator op_;
  const record_type& layout_;
  std::string_view layout_name_;
};

/// Checks whether a [resolved](@ref type_extractor) expression matches a given
/// type. That is, this visitor tests whether an expression consists of a
/// viable set of predicates for a type. For conjunctions, all operands must
/// match. For disjunctions, at least one operand must match.
struct matcher {
  matcher(const type& t);

  bool operator()(caf::none_t);
  bool operator()(const conjunction&);
  bool operator()(const disjunction&);
  bool operator()(const negation&);
  bool operator()(const predicate&);
  bool operator()(const selector&, const data&);
  bool operator()(const data_extractor&, const data&);

  template <class T>
    requires(!std::is_same_v<T, data>)
  bool operator()(const data& d, const T& x) {
    return (*this)(x, d);
  }

  template <class T, class U>
  bool operator()(const T&, const U&) {
    return false;
  }

  const type& type_;
  relational_operator op_;
};

/// A visitor that labels expression nodes with [offsets](@ref offset). The
/// length of an `offset` is the depth of the corresponding node in the AST.
/// The value of an offset element corresponds to the orindal number of the
/// node with respect to its parent. The visitor traverses the expression
/// depth-first, pre-order.
template <class Function>
class labeler {
  // TODO: we could add a static_assert that the return type of Function must
  // be void.
public:
  explicit labeler(Function f) : f_{f} {
    push();
  }

  void operator()(caf::none_t x) {
    visit(x);
  }

  template <class T>
  void operator()(const T& xs) {
    static_assert(detail::is_any_v<T, conjunction, disjunction>);
    visit(xs);
    if (!xs.empty()) {
      push();
      caf::visit(*this, xs[0]);
      for (size_t i = 1; i < xs.size(); ++i) {
        next();
        caf::visit(*this, xs[i]);
      }
      pop();
    }
  }

  void operator()(const negation& x) {
    visit(x);
    push();
    caf::visit(*this, x.expr());
    pop();
  }

  void operator()(const predicate& x) {
    visit(x);
  }

private:
  template <class T>
  void visit(const T& x) {
    f_(x, offset_);
  }

  void push() {
    offset_.emplace_back(0);
  }

  void pop() {
    offset_.pop_back();
  }

  void next() {
    VAST_ASSERT(!offset_.empty());
    ++offset_.back();
  }

  Function f_;
  offset offset_;
};

} // namespace vast
