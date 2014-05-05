#ifndef VAST_EXPRESSION_H
#define VAST_EXPRESSION_H

#include "vast/event.h"
#include "vast/offset.h"
#include "vast/operator.h"
#include "vast/schema.h"
#include "vast/util/operators.h"
#include "vast/util/visitor.h"

namespace vast {
namespace expr {

// Forward declarations
struct constant;
struct timestamp_extractor;
struct name_extractor;
struct id_extractor;
struct offset_extractor;
struct schema_extractor;
struct type_extractor;
struct predicate;
struct conjunction;
struct disjunction;

using const_visitor = util::const_visitor<
  constant,
  timestamp_extractor,
  name_extractor,
  id_extractor,
  offset_extractor,
  schema_extractor,
  type_extractor,
  predicate,
  conjunction,
  disjunction
>;

struct default_const_visitor : expr::const_visitor
{
  virtual void visit(constant const&) { }
  virtual void visit(name_extractor const&) { }
  virtual void visit(timestamp_extractor const&) { }
  virtual void visit(id_extractor const&) { }
  virtual void visit(offset_extractor const&) { }
  virtual void visit(schema_extractor const&) { }
  virtual void visit(type_extractor const&) { }
  virtual void visit(predicate const&) { }
  virtual void visit(conjunction const&) { }
  virtual void visit(disjunction const&) { }
};

/// The base class for nodes in the expression tree.
struct node : public util::visitable_with<const_visitor>,
              util::totally_ordered<node>
{
  virtual ~node() = default;
  virtual node* clone() const = 0;
  virtual bool equals(node const& other) const = 0;
  virtual bool is_less_than(node const& other) const = 0;
  virtual void serialize(serializer& sink) const = 0;
  virtual void deserialize(deserializer& source) = 0;
  friend bool operator==(node const& x, node const& y);
  friend bool operator<(node const& x, node const& y);
};

trial<void> convert(node const& n, std::string& s, bool tree = false);

/// A constant value.
struct constant : public util::visitable<node, constant, const_visitor>
{
  constant() = default;
  constant(value v);
  virtual constant* clone() const override;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;

  value val;
};

/// The base class for extractor nodes.
struct extractor : public util::abstract_visitable<node, const_visitor>
{
  extractor* clone() const = 0;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
};

/// Extracts the event timestamp.
struct timestamp_extractor
  : public util::visitable<extractor, timestamp_extractor, const_visitor>
{
  timestamp_extractor* clone() const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;
};

/// Extracts the event name.
struct name_extractor
  : public util::visitable<extractor, name_extractor, const_visitor>
{
  name_extractor* clone() const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;
};

/// Extracts the event ID.
struct id_extractor
  : public util::visitable<extractor, id_extractor, const_visitor>
{
  id_extractor* clone() const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;
};

/// Extract a value at a given offset.
struct offset_extractor
  : public util::visitable<extractor, offset_extractor, const_visitor>
{
  offset_extractor();
  offset_extractor(type_const_ptr type, offset off);

  virtual offset_extractor* clone() const override;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;

  type_const_ptr type;
  offset off;
};

/// Extracts arguments according to a schema.
struct schema_extractor
  : public util::visitable<extractor, schema_extractor, const_visitor>
{
  schema_extractor() = default;
  schema_extractor(key k);

  virtual schema_extractor* clone() const override;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;

  vast::key key;
};

/// Extracts arguments of a given type.
struct type_extractor
  : public util::visitable<extractor, type_extractor, const_visitor>
{
  type_extractor() = default;
  type_extractor(value_type t);

  virtual type_extractor* clone() const override;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;

  value_type type;
};

/// An n-ary operator.
struct n_ary_operator : public util::abstract_visitable<node, const_visitor>
{
  n_ary_operator() = default;
  n_ary_operator(n_ary_operator const& other);
  virtual n_ary_operator* clone() const = 0;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;
  void add(std::unique_ptr<node> n);

  std::vector<std::unique_ptr<node>> operands;
};

/// A predicate.
struct predicate
  : public util::visitable<n_ary_operator, predicate, const_visitor>
{
  using binary_predicate = std::function<bool(value const&, value const&)>;
  static binary_predicate make_predicate(relational_operator op);

  predicate() = default;
  predicate(relational_operator op);

  node const& lhs() const;
  node const& rhs() const;

  virtual predicate* clone() const override;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;

  binary_predicate pred;
  relational_operator op;
};

/// A conjunction.
struct conjunction
  : public util::visitable<n_ary_operator, conjunction, const_visitor>
{
  virtual conjunction* clone() const override;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;
};

/// A disjunction.
struct disjunction
  : public util::visitable<n_ary_operator, disjunction, const_visitor>
{
  virtual disjunction* clone() const override;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;
};

/// A wrapper around an expression node with value semantics.
class ast : util::totally_ordered<ast>
{
public:
  /// Creates an AST.
  /// @param str The string representing the expression.
  static trial<ast> parse(std::string const& str);

  ast() = default;
  ast(std::string const& str);
  ast(std::unique_ptr<node> n);
  ast(node const& n);
  ast(ast const& other);
  ast(ast&&) = default;
  ast& operator=(ast const& other);
  ast& operator=(ast&& other);
  explicit operator bool() const;

  void accept(const_visitor& v);
  void accept(const_visitor& v) const;

  node const* root() const;

  //
  // Introspection
  //
  bool is_conjunction() const;
  bool is_disjunction() const;
  bool is_predicate() const;
  bool is_meta_predicate() const;
  bool is_time_predicate() const;
  bool is_name_predicate() const;
  value const* find_constant() const;
  offset const* find_offset() const;
  relational_operator const* find_operator() const;

  //
  // Transformation
  //

  // Extracts all (leaf) predicates from an AST.
  // @returns All leaf predicates of *a*.
  std::vector<ast> predicatize() const;

  /// Transforms a schema extractor into a sequence of offset extractors.
  /// @param sch The schema used to resolve schema predicates.
  /// @returns This AST with offset extractors instead of schema extractors.
  trial<ast> resolve(schema const& sch) const;

private:
  std::unique_ptr<node> node_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);
  friend bool operator<(ast const& x, ast const& y);
  friend bool operator==(ast const& x, ast const& y);

  template <typename Iterator>
  friend trial<void> print(ast const& a, Iterator&& out, bool tree = false)
  {
    // FIXME: don't use poor man's printing via string generation.
    auto str = to<std::string>(a, tree);
    if (! str)
      return str.error();

    return print(*str, out);
  }

  friend trial<void> convert(ast const& a, std::string& s, bool tree = false)
  {
    return a.node_ ? convert(*a.node_, s, tree) : nothing;
  }
};

/// Evaluates an expression node for a given event.
value evaluate(node const& n, event const& e);
value evaluate(ast const& a, event const& e);

} // namespace expr
} // namespace vast

#endif
