#ifndef VAST_EXPRESSION_H
#define VAST_EXPRESSION_H

#include "vast/config.h"
#include "vast/event.h"
#include "vast/offset.h"
#include "vast/operator.h"
#include "vast/optional.h"
#include "vast/print.h"
#include "vast/regex.h"
#include "vast/schema.h"
#include "vast/detail/ast/query.h"
#include "vast/detail/parser/error_handler.h"
#include "vast/detail/parser/skipper.h"
#include "vast/detail/parser/query.h"
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
  schema_extractor(vast::key k);

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
  type_extractor(type_tag t);

  virtual type_extractor* clone() const override;
  virtual bool equals(node const& other) const override;
  virtual bool is_less_than(node const& other) const override;
  virtual void serialize(serializer& sink) const override;
  virtual void deserialize(deserializer& source) override;

  type_tag type;
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
  ast() = default;
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
    std::string str;
    auto t = convert(*a.node_, str, tree);
    if (! t)
      return t.error();

#ifdef VAST_GCC
    // FIXME: why do we need to pull in util::print to enable ADL? Shouldn't an
    // unqualified call work directly?
    using util::print;
#endif
    return print(str, out);
  }
};

namespace impl {

// Takes a query AST and generates a polymorphic query expression tree.
class expressionizer
{
public:
  using result_type = void;

  static trial<std::unique_ptr<node>>
  apply(detail::ast::query::query const& q)
  {
    std::unique_ptr<n_ary_operator> root;

    if (q.rest.empty())
    {
      // WLOG, we can always add a conjunction as parent if we just have a
      // single predicate.
      root = std::make_unique<conjunction>();

      expressionizer visitor{root.get()};
      boost::apply_visitor(std::ref(visitor), q.first);
      if (visitor.error_)
        return std::move(*visitor.error_);

      return std::unique_ptr<node>(std::move(root)); // FIXME: add to parent.
    }

    // First, split the query expression at each OR node.
    std::vector<detail::ast::query::query> ors;
    ors.push_back({q.first, {}});
    for (auto& pred : q.rest)
      if (pred.op == logical_or)
        ors.push_back({pred.operand, {}});
      else
        ors.back().rest.push_back(pred);

    // Our AST root will be a disjunction iff we have at least two terms.
    if (ors.size() >= 2)
      root = std::make_unique<disjunction>();

    // Then create a conjunction for each set of subsequent AND nodes between
    // two OR nodes.
    std::unique_ptr<conjunction> conj;
    for (auto& ands : ors)
    {
      n_ary_operator* local_root;
      if (! root)
      {
        root = std::make_unique<conjunction>();
        local_root = root.get();
      }
      else if (! ands.rest.empty())
      {
        auto conj = std::make_unique<conjunction>();
        local_root = conj.get();
        root->add(std::move(conj));
      }
      else
      {
        local_root = root.get();
      }

      expressionizer visitor{local_root};
      boost::apply_visitor(std::ref(visitor), ands.first);
      if (visitor.error_)
        return std::move(*visitor.error_);

      for (auto pred : ands.rest)
      {
        boost::apply_visitor(std::ref(visitor), pred.operand);
        if (visitor.error_)
          return std::move(*visitor.error_);
      }
    }

    return std::unique_ptr<node>(std::move(root));
  }

  expressionizer(n_ary_operator* parent)
    : parent_(parent)
  {
  }

  void operator()(detail::ast::query::query const& q)
  {
    auto n = apply(q);
    if (n)
      parent_->add(std::move(*n));
    else
      error_ = n.error();
  }

  void operator()(detail::ast::query::predicate const& operand)
  {
    boost::apply_visitor(*this, operand);
  }

  void operator()(detail::ast::query::tag_predicate const& pred)
  {
    auto op = pred.op;
    if (invert_)
    {
      op = negate(op);
      invert_ = false;
    }

    std::unique_ptr<extractor> lhs;
    if (pred.lhs == "name")
      lhs = std::make_unique<name_extractor>();
    else if (pred.lhs == "time")
      lhs = std::make_unique<timestamp_extractor>();
    else if (pred.lhs == "id")
      lhs = std::make_unique<id_extractor>();

    auto rhs = std::make_unique<constant>(detail::ast::query::fold(pred.rhs));
    auto p = std::make_unique<predicate>(op);
    p->add(std::move(lhs));
    p->add(std::move(rhs));

    parent_->add(std::move(p));
  }

  void operator()(detail::ast::query::type_predicate const& pred)
  {
    auto op = pred.op;
    if (invert_)
    {
      op = negate(op);
      invert_ = false;
    }

    auto lhs = std::make_unique<type_extractor>(pred.lhs);
    auto rhs = std::make_unique<constant>(detail::ast::query::fold(pred.rhs));
    auto p = std::make_unique<predicate>(op);
    p->add(std::move(lhs));
    p->add(std::move(rhs));

    parent_->add(std::move(p));
  }

  void operator()(detail::ast::query::schema_predicate const& pred)
  {
    auto op = pred.op;
    if (invert_)
    {
      op = negate(op);
      invert_ = false;
    }

    key k;
    for (auto& str : pred.lhs)
      k.emplace_back(str);

    auto lhs = std::make_unique<schema_extractor>(std::move(k));
    auto rhs = std::make_unique<constant>(detail::ast::query::fold(pred.rhs));
    auto p = std::make_unique<predicate>(op);
    p->add(std::move(lhs));
    p->add(std::move(rhs));

    parent_->add(std::move(p));
  }

  void operator()(detail::ast::query::negated_predicate const& pred)
  {
    // Since all operators have a complement, we can push down the negation to
    // the operator-level (as opposed to leaving it at the predicate level).
    invert_ = true;
    boost::apply_visitor(*this, pred.operand);
  }

private:
  std::unique_ptr<offset_extractor> make_offset_extractor(type_const_ptr t,
                                                          offset o)
  {
    return std::make_unique<offset_extractor>(t, std::move(o));
  }

  std::unique_ptr<constant>
  make_constant(detail::ast::query::value_expr const& expr)
  {
    return std::make_unique<constant>(detail::ast::query::fold(expr));
  }

  std::unique_ptr<node> make_glob_node(std::string const& expr)
  {
    // Determine whether we need a regular expression node or whether basic
    // equality comparison suffices. This check is relatively crude at the
    // moment: we just look whether the expression contains * or ?.
    auto glob = regex("\\*|\\?").search(expr);
    auto p = std::make_unique<predicate>(glob ? match : equal);
    auto lhs = std::make_unique<name_extractor>();
    p->add(std::move(lhs));
    if (glob)
      p->add(std::make_unique<constant>(regex::glob(expr)));
    else
      p->add(std::make_unique<constant>(expr));
    return std::move(p);
  }

  n_ary_operator* parent_;
  bool invert_ = false;
  optional<error> error_;
};

} // namespace impl

template <typename Iterator>
trial<void> parse(ast& a, Iterator& begin, Iterator end)
{
  std::string err;
  detail::parser::error_handler<Iterator> on_error{begin, end, err};
  detail::parser::query<Iterator> grammar{on_error};
  detail::parser::skipper<Iterator> skipper;
  detail::ast::query::query q;

  bool success = phrase_parse(begin, end, grammar, skipper, q);
  if (! success)
    return error{std::move(err)};
  else if (begin != end)
    return error{"input not consumed:'", std::string{begin, end}, "'"};

  if (! detail::ast::query::validate(q))
    return error{"failed validation"};

  auto t = impl::expressionizer::apply(q);
  if (! t)
    return t.error();

  a = ast{std::move(*t)};
  return nothing;
}

/// Checks whether the types of two nodes in a predicate are compatible with
/// each other, i.e., whether operator evaluation for the given types is
/// semantically correct. Note that this function assumes the AST has already
/// been normalized with the type of the extractor occurring at the LHS and the
/// value at the RHS.
///
/// @param lhs The type of the extractor.
///
/// @param rhs The type of the value.
///
/// @param op The operator under which to compare *lhs* and *rhs*.
///
/// @returns `true` if *lhs* and *rhs* are compatible to each other under *op*.
bool compatible(type_tag lhs, type_tag rhs, relational_operator op);

/// Evaluates an expression node for a given event.
value evaluate(node const& n, event const& e);
value evaluate(ast const& a, event const& e);

} // namespace expr
} // namespace vast

#endif
