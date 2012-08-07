#ifndef VAST_EXPRESSION_H
#define VAST_EXPRESSION_H

#include <ze/event.h>
#include "vast/util/visitor.h"

namespace vast {
namespace expr {

class node;
class timestamp_extractor;
class name_extractor;
class id_extractor;
class offset_extractor;
class exists;
class conjunction;
class disjunction;
class relational_operator;
class constant;

typedef util::const_visitor<
    node
 ,  timestamp_extractor
 ,  name_extractor
 ,  id_extractor
 ,  offset_extractor
 ,  exists
 ,  conjunction
 ,  disjunction
 ,  relational_operator
 ,  constant
> const_visitor;

typedef util::visitor<
    node
 ,  timestamp_extractor
 ,  name_extractor
 ,  id_extractor
 ,  offset_extractor
 ,  exists
 ,  conjunction
 ,  disjunction
 ,  relational_operator
 ,  constant
> visitor;

/// The base class for nodes in the expression tree.
class node
{
  node(node const&) = delete;

public:
  virtual ~node() = default;

  /// Gets the result of the sub-tree induced by this node.
  /// @return The value of this node.
  ze::value const& result() const;

  /// Determines whether the result is available without evaluation.
  ///
  /// @return `true` if the result can be obtained without a call to
  /// node::eval.
  bool ready() const;

  /// Resets the sub-tree induced by this node.
  virtual void reset();

  /// Evaluates the sub-tree induced by this node.
  virtual void eval() = 0;

  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

protected:
  node() = default;

  ze::value result_ = ze::invalid;
  bool ready_ = false;
};

/// The base class for extractor nodes.
class extractor : public node
{
public:
  virtual void feed(ze::event const* event);
  ze::event const* event() const;

  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

protected:
  virtual void eval() = 0;

  ze::event const* event_;
};

/// Extracts the event timestamp.
class timestamp_extractor : public extractor
{
public:
  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

private:
  virtual void eval();
};

/// Extracts the event name.
class name_extractor : public extractor
{
public:
  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

private:
  virtual void eval();
};

/// Extracts the event ID.
class id_extractor : public extractor
{
public:
  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

private:
  virtual void eval();
};

/// Extracts an argument at a given offset.
class offset_extractor : public extractor
{
public:
  offset_extractor(std::vector<size_t> offsets);

  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

private:
  virtual void eval();
  std::vector<size_t> offsets_;
};

/// An existential quantifier.
class exists : public extractor
{
public:
  exists(ze::value_type type);

  virtual void feed(ze::event const* event);
  virtual void reset();

  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

private:
  virtual void eval();

  ze::value_type type_;
  size_t current_ = 0;
  size_t flat_size_ = 0;
};

/// An n-ary operator.
class n_ary_operator : public node
{
public:
  void add(std::unique_ptr<node> operand);
  virtual void reset();

  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

  std::vector<std::unique_ptr<node>>& operands();
  std::vector<std::unique_ptr<node>> const& operands() const;

protected:
  virtual void eval() = 0;
  std::vector<std::unique_ptr<node>> operands_;
};

/// A conjunction.
class conjunction : public n_ary_operator
{
public:
  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

private:
  virtual void eval();
};

/// A disjunction.
class disjunction : public n_ary_operator
{
public:
  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

private:
  virtual void eval();
};

/// The type of a relational operator.
enum relation_type
{
  match,
  not_match,
  in,
  not_in,
  equal,
  not_equal,
  less,
  less_equal,
  greater,
  greater_equal
};

/// A relational operator.
class relational_operator : public n_ary_operator
{
public:
  typedef std::function<bool(ze::value const&, ze::value const&)>
    binary_predicate;

  relational_operator(relation_type op);

  bool test(ze::value const& lhs, ze::value const& rhs) const;
  relation_type type() const;

  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

private:
  virtual void eval();

  binary_predicate op_;
  relation_type type_;
};

/// A constant value.
class constant : public node
{
public:
  constant(ze::value value);
  virtual void reset();

  VAST_ACCEPT_CONST(const_visitor)
  VAST_ACCEPT(visitor)

private:
  virtual void eval();
};

} // namespace expr

/// A query expression.
class expression
{
public:
  /// Constructs an empty expression.
  expression() = default;

  /// Copy-constructs an expression
  /// @param other The expression to copy.
  expression(expression const& other);

  /// Move-constructs an expression
  /// @param other The expression to move.
  expression(expression&& other);

  /// Assigns an expression.
  /// @param other The RHS of the assignment
  expression& operator=(expression other);

  /// Parses a given expression.
  /// @param str The query expression to transform into an AST.
  void parse(std::string str);

  /// Evaluates an event with respect to the root node.
  /// @param event The event to evaluate against the expression.
  /// @return `true` if @a event matches the expression.
  bool eval(ze::event const& event);

  /// Allow a visitor to process the expression.
  /// @param v The visitor
  void accept(expr::const_visitor& v) const;

  /// Allow a visitor to process the expression.
  /// @param v The visitor
  void accept(expr::visitor& v);

private:
  template <typename Archive>
  friend void serialize(Archive& oa, expression const& expr)
  {
    oa << expr.str_;
  }

  template <typename Archive>
  friend void deserialize(Archive& ia, expression& expr)
  {
    std::string str;
    ia >> str;
    expr.parse(std::move(str));
  }

  friend bool operator==(expression const& x, expression const& y);

  std::string str_;
  std::unique_ptr<expr::node> root_;
  std::vector<expr::extractor*> extractors_;
};

} // namespace vast

#endif
