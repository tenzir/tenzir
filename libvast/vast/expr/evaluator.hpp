#ifndef VAST_EXPR_EVALUATOR_HPP
#define VAST_EXPR_EVALUATOR_HPP

#include "vast/expression.hpp"

namespace vast {

class event;

namespace expr {

/// Evaluates an event over a resolved expression.
struct event_evaluator {
  event_evaluator(event const& e);

  bool operator()(none);
  bool operator()(conjunction const& c);
  bool operator()(disjunction const& d);
  bool operator()(negation const& n);
  bool operator()(predicate const& p);
  bool operator()(event_extractor const&, data const& d);
  bool operator()(time_extractor const&, data const& d);
  bool operator()(type_extractor const&, data const&);
  bool operator()(schema_extractor const&, data const&);
  bool operator()(data_extractor const& e, data const& d);

  template <typename T>
  bool operator()(data const& d, T const& e) {
    return (*this)(e, d);
  }

  template <typename T, typename U>
  bool operator()(T const&, U const&) {
    return false;
  }

  event const& event_;
  relational_operator op_;
};

/// Base class for expression evaluators operating on bitstreams.
/// @tparam Derived The CRTP client.
/// @tparam Bitstream The type of bitstream used during evaluation.
template <typename Derived, typename Bitstream>
struct bitstream_evaluator {
  Bitstream operator()(none) const {
    return {};
  }

  Bitstream operator()(conjunction const& con) const {
    auto hits = visit(*this, con[0]);
    if (hits.empty() || hits.all_zeros())
      return {};
    for (size_t i = 1; i < con.size(); ++i) {
      hits &= visit(*this, con[i]);
      if (hits.empty() || hits.all_zeros()) // short-circuit
        return {};
    }
    return hits;
  }

  Bitstream operator()(disjunction const& dis) const {
    Bitstream hits;
    for (auto& op : dis) {
      hits |= visit(*this, op);
      if (!hits.empty() && hits.all_ones()) // short-circuit
        break;
    }
    return hits;
  }

  Bitstream operator()(negation const& n) const {
    auto hits = visit(*this, n.expression());
    hits.flip();
    return hits;
  }

  Bitstream operator()(predicate const& pred) const {
    auto* bs = static_cast<Derived const*>(this)->lookup(pred);
    return bs ? *bs : Bitstream{};
  }
};

} // namespace expr
} // namespace vast

#endif
