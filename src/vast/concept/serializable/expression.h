#ifndef VAST_CONCEPT_SERIALIZABLE_EXPRESSION_H
#define VAST_CONCEPT_SERIALIZABLE_EXPRESSION_H

// TODO: make expression model the state concept instead.

#include "vast/expression.h"
#include "vast/concept/serializable/data.h"
#include "vast/concept/serializable/state.h"
#include "vast/concept/serializable/type.h"
#include "vast/concept/serializable/std/string.h"
#include "vast/concept/serializable/std/vector.h"
#include "vast/concept/serializable/util/variant.h"
#include "vast/concept/state/expression.h"

namespace vast {
namespace detail {

template <typename Serializer>
struct expr_serializer
{
  expr_serializer(Serializer& sink)
    : sink_{sink}
  {
  }

  template <typename T>
  void operator()(T const&)
  {
  }

  void operator()(conjunction const& c)
  {
    sink_ << static_cast<std::vector<expression> const&>(c);
  }

  void operator()(disjunction const& d)
  {
    sink_ << static_cast<std::vector<expression> const&>(d);
  }

  void operator()(negation const& n)
  {
    sink_ << static_cast<std::vector<expression> const&>(n);
  }

  void operator()(predicate const& p)
  {
    sink_ << p;
  }

  void operator()(type_extractor const& t)
  {
    sink_ << t.type;
  }

  void operator()(schema_extractor const& e)
  {
    sink_ << e.key;
  }

  void operator()(data_extractor const& e)
  {
    sink_ << e.type << e.offset;
  }

  void operator()(data const& d)
  {
    sink_ << d;
  }

  Serializer& sink_;
};

template <typename Deserializer>
struct expr_deserializer
{
  expr_deserializer(Deserializer& source)
    : source_{source}
  {
  }

  template <typename T>
  void operator()(T&)
  {
  }

  void operator()(conjunction& c)
  {
    source_ >> static_cast<std::vector<expression>&>(c);
  }

  void operator()(disjunction& d)
  {
    source_ >> static_cast<std::vector<expression>&>(d);
  }

  void operator()(negation& n)
  {
    source_ >> static_cast<std::vector<expression>&>(n);
  }

  void operator()(predicate& p)
  {
    source_ >> p;
  }

  void operator()(type_extractor& t)
  {
    source_ >> t.type;
  }

  void operator()(schema_extractor& e)
  {
    source_ >> e.key;
  }

  void operator()(data_extractor& e)
  {
    source_ >> e.type >> e.offset;
  }

  void operator()(data& d)
  {
    source_ >> d;
  }

  Deserializer& source_;
};

} // namespace <anonymous>

template <typename Serializer>
void serialize(Serializer& sink, predicate const& p)
{
  sink << which(p.lhs);
  visit(detail::expr_serializer<Serializer>{sink}, p.lhs);
  sink << p.op;
  sink << which(p.rhs);
  visit(detail::expr_serializer<Serializer>{sink}, p.rhs);
}

template <typename Deserializer>
void deserialize(Deserializer& source, predicate& p)
{
  predicate::operand::tag l;
  source >> l;
  p.lhs = predicate::operand::make(l);
  visit(detail::expr_deserializer<Deserializer>{source}, p.lhs);
  source >> p.op;
  predicate::operand::tag r;
  source >> r;
  p.rhs = predicate::operand::make(r);
  visit(detail::expr_deserializer<Deserializer>{source}, p.rhs);
}

template <typename Serializer>
void serialize(Serializer& sink, expression const& expr)
{
  sink << which(expr);
  visit(detail::expr_serializer<Serializer>{sink}, expr);
}

template <typename Deserializer>
void deserialize(Deserializer& source, expression& expr)
{
  auto f = [&](auto& x)
  {
    expression::node::tag t;
    source >> t;
    x = expression::node::make(t);
    visit(detail::expr_deserializer<Deserializer>{source}, expr);
  };
  access::state<expression>::call(expr, f);
}

} // namespace vast

#endif
