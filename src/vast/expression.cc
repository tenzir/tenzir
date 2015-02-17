#include "vast/expression.h"

#include "vast/serialization/container.h"
#include "vast/serialization/variant.h"

namespace vast {
namespace {

struct expr_serializer
{
  expr_serializer(serializer& sink)
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

  serializer& sink_;
};

struct expr_deserializer
{
  expr_deserializer(deserializer& source)
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

  deserializer& source_;
};

} // namespace <anonymous>

void serialize(serializer& sink, predicate const& p)
{
  sink << which(p.lhs);
  visit(expr_serializer{sink}, p.lhs);
  sink << p.op;
  sink << which(p.rhs);
  visit(expr_serializer{sink}, p.rhs);
}

void deserialize(deserializer& source, predicate& p)
{
  predicate::operand::tag l;
  source >> l;
  p.lhs = predicate::operand::make(l);
  visit(expr_deserializer{source}, p.lhs);
  source >> p.op;
  predicate::operand::tag r;
  source >> r;
  p.rhs = predicate::operand::make(r);
  visit(expr_deserializer{source}, p.rhs);
}

expression const& negation::expression() const
{
  assert(! empty());
  return *begin();
}

expression& negation::expression()
{
  assert(! empty());
  return *begin();
}


void expression::serialize(serializer& sink) const
{
  sink << which(node_);
  visit(expr_serializer{sink}, node_);
}


void expression::deserialize(deserializer& source)
{
  node::tag t;
  source >> t;
  node_ = node::make(t);
  visit(expr_deserializer{source}, node_);
}

expression::node& expose(expression& e)
{
  return e.node_;
}

expression::node const& expose(expression const& e)
{
  return e.node_;
}

bool operator==(expression const& lhs, expression const& rhs)
{
  return lhs.node_ == rhs.node_;
}

bool operator<(expression const& lhs, expression const& rhs)
{
  return lhs.node_ < rhs.node_;
}

} // namespace vast
