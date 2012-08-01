#ifndef VAST_EXPRESSION_H
#define VAST_EXPRESSION_H

#include <ze/event.h>

namespace vast {

// TODO: Hide the detail aspects in the implementation.
// Forward declarations
namespace detail {
namespace ast {
enum clause_operator : int;
struct query;
} // namespace ast
} // namespace detail

namespace expr {

/// The base class for nodes in the expression tree.
class node
{
  node(node const&) = delete;

public:
  virtual ~node() = default;

  /// Gets the result of the sub-tree induced by this node.
  /// @return The value of this node.
  ze::value const& result();

  /// Determines whether the result is available without evaluation.
  ///
  /// @return `true` if the result can be obtained without a call to
  /// node::eval.
  bool ready() const;

  /// Resets the sub-tree induced by this node.
  virtual void reset();

protected:
  node() = default;
  virtual void eval() = 0;

  ze::value result_ = ze::invalid;
  bool ready_ = false;
};

/// The base class for extractor nodes.
class extractor : public node
{
public:
  virtual void feed(ze::event const* event);

protected:
  virtual void eval() = 0;

  ze::event const* event_;
};

/// Extracts the event timestamp.
class timestamp_extractor : public extractor
{
private:
  virtual void eval();
};

/// Extracts the event name.
class name_extractor : public extractor
{
private:
  virtual void eval();
};

/// Extracts the event ID.
class id_extractor : public extractor
{
private:
  virtual void eval();
};

/// Extracts an argument at a given offset.
class offset_extractor : public extractor
{
public:
  offset_extractor(size_t offset);

private:
  virtual void eval();
  size_t offset_;
};

/// An existential quantifier.
class exists : public extractor
{
public:
  exists(ze::value_type type);

  virtual void feed(ze::event const* event);
  virtual void reset();

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

protected:
  virtual void eval() = 0;
  std::vector<std::unique_ptr<node>> operands_;
};

/// A conjunction.
class conjunction : public n_ary_operator
{
private:
  virtual void eval();
};

/// A disjunction.
class disjunction : public n_ary_operator
{
private:
  virtual void eval();
};

// TODO: factor into one class for each operator.
/// A binary operator.
class relational_operator : public n_ary_operator
{
  typedef std::function<bool(ze::value const&, ze::value const&)>
    binary_predicate;
public:
  relational_operator(detail::ast::clause_operator op);

private:
  virtual void eval();

  binary_predicate op_;
};

/// A constant value.
class constant : public node
{
public:
  constant(ze::value value);
  virtual void reset();

private:
  virtual void eval();
};

} // namespace expr

/// A query expression.
class expression
{
  expression(expression const&) = delete;

public:
  expression() = default;

  /// Creates an expression tree from a query AST.
  /// @param query A parsed (and validated) query AST.
  void assign(detail::ast::query const& query);

  /// Evaluates an event with respect to the root node.
  /// @param event The event to evaluate against the expression.
  /// @return `true` if @a event matches the expression.
  bool eval(ze::event const& event);

private:
  std::unique_ptr<expr::node> root_;
  std::vector<expr::extractor*> extractors_;
};

} // namespace vast

#endif
