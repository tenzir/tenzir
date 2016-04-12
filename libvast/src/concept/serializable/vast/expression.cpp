#include "vast/concept/serializable/state.hpp"
#include "vast/concept/serializable/vast/data.hpp"
#include "vast/concept/serializable/vast/expression.hpp"
#include "vast/concept/serializable/vast/type.hpp"
#include "vast/concept/serializable/vast/util/variant.hpp"
#include "vast/concept/state/expression.hpp"

// TODO: make expression model the state concept instead.

namespace vast {
namespace {

template <class Serializer>
struct expr_serializer {
  expr_serializer(Serializer& sink) : sink_{sink} {
  }

  template <class T>
  void operator()(T const&) {
  }

  void operator()(conjunction const& c) {
    sink_ << static_cast<std::vector<expression> const&>(c);
  }

  void operator()(disjunction const& d) {
    sink_ << static_cast<std::vector<expression> const&>(d);
  }

  void operator()(negation const& n) {
    sink_ << static_cast<std::vector<expression> const&>(n);
  }

  void operator()(predicate const& p) {
    sink_ << p;
  }

  void operator()(type_extractor const& t) {
    sink_ << t.type;
  }

  void operator()(schema_extractor const& e) {
    sink_ << e.key;
  }

  void operator()(data_extractor const& e) {
    sink_ << e.type << e.offset;
  }

  void operator()(data const& d) {
    sink_ << d;
  }

  Serializer& sink_;
};

template <class Deserializer>
struct expr_deserializer {
  expr_deserializer(Deserializer& source) : source_{source} {
  }

  template <class T>
  void operator()(T&) {
  }

  void operator()(conjunction& c) {
    source_ >> static_cast<std::vector<expression>&>(c);
  }

  void operator()(disjunction& d) {
    source_ >> static_cast<std::vector<expression>&>(d);
  }

  void operator()(negation& n) {
    source_ >> static_cast<std::vector<expression>&>(n);
  }

  void operator()(predicate& p) {
    source_ >> p;
  }

  void operator()(type_extractor& t) {
    source_ >> t.type;
  }

  void operator()(schema_extractor& e) {
    source_ >> e.key;
  }

  void operator()(data_extractor& e) {
    source_ >> e.type >> e.offset;
  }

  void operator()(data& d) {
    source_ >> d;
  }

  Deserializer& source_;
};

} // namespace <anonymous>

void serialize(caf::serializer& sink, predicate const& p) {
  sink << which(p.lhs);
  visit(expr_serializer<caf::serializer>{sink}, p.lhs);
  sink << p.op;
  sink << which(p.rhs);
  visit(expr_serializer<caf::serializer>{sink}, p.rhs);
}

void serialize(caf::deserializer& source, predicate& p) {
  predicate::operand::tag l;
  source >> l;
  p.lhs = predicate::operand::make(l);
  visit(expr_deserializer<caf::deserializer>{source}, p.lhs);
  source >> p.op;
  predicate::operand::tag r;
  source >> r;
  p.rhs = predicate::operand::make(r);
  visit(expr_deserializer<caf::deserializer>{source}, p.rhs);
}

void serialize(caf::serializer& sink, expression const& expr) {
  sink << which(expr);
  visit(expr_serializer<caf::serializer>{sink}, expr);
}

void serialize(caf::deserializer& source, expression& expr) {
  auto f = [&](auto& x) {
    expression::node::tag t;
    source >> t;
    x = expression::node::make(t);
    visit(expr_deserializer<caf::deserializer>{source}, expr);
  };
  access::state<expression>::call(expr, f);
}

} // namespace vast
