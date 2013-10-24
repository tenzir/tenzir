#include "vast/expression.h"

#include <boost/variant/apply_visitor.hpp>
#include "vast/convert.h"
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/optional.h"
#include "vast/regex.h"
#include "vast/serialization.h"
#include "vast/detail/ast/query.h"
#include "vast/detail/parser/error_handler.h"
#include "vast/detail/parser/skipper.h"
#include "vast/detail/parser/query.h"
#include "vast/util/make_unique.h"

namespace vast {
namespace expr {

bool operator==(node const& x, node const& y)
{
  return x.equals(y);
}

bool operator<(node const& x, node const& y)
{
  return x.is_less_than(y);
}

constant::constant(value v)
  : val(std::move(v))
{
}

constant* constant::clone() const
{
  return new constant{*this};
}

bool constant::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  return val == static_cast<constant const&>(other).val;
}

bool constant::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();
  return val < static_cast<constant const&>(other).val;
}

void constant::serialize(serializer& sink) const
{
  sink << val;
}

void constant::deserialize(deserializer& source)
{
  source >> val;
}

bool extractor::equals(node const& other) const
{
  return (typeid(*this) == typeid(other));
}

bool extractor::is_less_than(node const& other) const
{
  return typeid(*this).hash_code() < typeid(other).hash_code();
}


timestamp_extractor* timestamp_extractor::clone() const
{
  return new timestamp_extractor{*this};
}

void timestamp_extractor::serialize(serializer&) const
{
}

void timestamp_extractor::deserialize(deserializer&)
{
}


name_extractor* name_extractor::clone() const
{
  return new name_extractor{*this};
}

void name_extractor::serialize(serializer&) const
{
}

void name_extractor::deserialize(deserializer&)
{
}


id_extractor* id_extractor::clone() const
{
  return new id_extractor{*this};
}

void id_extractor::serialize(serializer&) const
{
}

void id_extractor::deserialize(deserializer&)
{
}


offset_extractor::offset_extractor(offset o)
  : off(std::move(o))
{
}

offset_extractor* offset_extractor::clone() const
{
  return new offset_extractor{*this};
}

void offset_extractor::serialize(serializer& sink) const
{
  sink << off;
}

void offset_extractor::deserialize(deserializer& source)
{
  source >> off;
}

bool offset_extractor::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  return off == static_cast<offset_extractor const&>(other).off;
}

bool offset_extractor::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();
  return off < static_cast<offset_extractor const&>(other).off;
}


type_extractor::type_extractor(value_type t)
  : type{t}
{
}

type_extractor* type_extractor::clone() const
{
  return new type_extractor{*this};
}

bool type_extractor::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  return type == static_cast<type_extractor const&>(other).type;
}

bool type_extractor::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();
  return type < static_cast<type_extractor const&>(other).type;
}

void type_extractor::serialize(serializer& sink) const
{
  sink << type;
}

void type_extractor::deserialize(deserializer& source)
{
  source >> type;
}


n_ary_operator::n_ary_operator(n_ary_operator const& other)
{
  for (auto& o : other.operands)
    operands.emplace_back(o->clone());
}

bool n_ary_operator::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  auto& that = static_cast<n_ary_operator const&>(other);
  if (operands.size() != that.operands.size())
    return false;
  for (size_t i = 0; i < operands.size(); ++i)
    if (*operands[i] != *that.operands[i])
      return false;
  return true;
}

bool n_ary_operator::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();
  auto& that = static_cast<n_ary_operator const&>(other);
  return std::lexicographical_compare(
      operands.begin(), operands.end(),
      that.operands.begin(), that.operands.end(),
      [](std::unique_ptr<node> const& x, std::unique_ptr<node> const& y)
      {
        return *x < *y;
      });
}

void n_ary_operator::serialize(serializer& sink) const
{
  sink << operands;
}

void n_ary_operator::deserialize(deserializer& source)
{
  source >> operands;
}

void n_ary_operator::add(std::unique_ptr<node> n)
{
  operands.push_back(std::move(n));
}


relation::binary_predicate relation::make_predicate(relational_operator op)
{
  switch (op)
  {
    default:
      assert(! "invalid operator");
      return {};
    case match:
      return [](value const& lhs, value const& rhs) -> bool
      {
        if (lhs.which() != string_type || rhs.which() != regex_type)
          return false;

        return rhs.get<regex>().match(lhs.get<string>());
      };
    case not_match:
      return [](value const& lhs, value const& rhs) -> bool
      {
        if (lhs.which() != string_type || rhs.which() != regex_type)
          return false;

        return ! rhs.get<regex>().match(lhs.get<string>());
      };
    case in:
      return [](value const& lhs, value const& rhs) -> bool
      {
        if (lhs.which() == string_type &&
            rhs.which() == regex_type)
          return rhs.get<regex>().search(lhs.get<string>());

        if (lhs.which() == address_type &&
            rhs.which() == prefix_type)
          return rhs.get<prefix>().contains(lhs.get<address>());

        return false;
      };
    case not_in:
      return [](value const& lhs, value const& rhs) -> bool
      {
        if (lhs.which() == string_type &&
            rhs.which() == regex_type)
          return ! rhs.get<regex>().search(lhs.get<string>());

        if (lhs.which() == address_type &&
            rhs.which() == prefix_type)
          return ! rhs.get<prefix>().contains(lhs.get<address>());

        return false;
      };
    case equal:
      return [](value const& lhs, value const& rhs)
      {
        return lhs == rhs;
      };
    case not_equal:
      return [](value const& lhs, value const& rhs)
      {
        return lhs != rhs;
      };
    case less:
      return [](value const& lhs, value const& rhs)
      {
        return lhs < rhs;
      };
    case less_equal:
      return [](value const& lhs, value const& rhs)
      {
        return lhs <= rhs;
      };
    case greater:
      return [](value const& lhs, value const& rhs)
      {
        return lhs > rhs;
      };
    case greater_equal:
      return [](value const& lhs, value const& rhs)
      {
        return lhs >= rhs;
      };
  }
}

relation::relation(relational_operator op)
  : op{op}
{
  predicate = make_predicate(op);
}

relation* relation::clone() const
{
  return new relation{*this};
}

bool relation::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  return op == static_cast<relation const&>(other).op
      && n_ary_operator::equals(other);
}

bool relation::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();
  auto& that = static_cast<n_ary_operator const&>(other);
  return op < static_cast<relation const&>(other).op
      || std::lexicographical_compare(
          operands.begin(), operands.end(),
          that.operands.begin(), that.operands.end(),
          [](std::unique_ptr<node> const& x, std::unique_ptr<node> const& y)
          {
            return *x < *y;
          });
}

void relation::serialize(serializer& sink) const
{
  n_ary_operator::serialize(sink);
  sink << op;
}

void relation::deserialize(deserializer& source)
{
  n_ary_operator::deserialize(source);
  source >> op;
  predicate = make_predicate(op);
}

conjunction* conjunction::clone() const
{
  return new conjunction{*this};
}

bool conjunction::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  return n_ary_operator::equals(other);
}

bool conjunction::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();
  auto& that = static_cast<n_ary_operator const&>(other);
  return std::lexicographical_compare(
      operands.begin(), operands.end(),
      that.operands.begin(), that.operands.end(),
      [](std::unique_ptr<node> const& x, std::unique_ptr<node> const& y)
      {
        return *x < *y;
      });
}

void conjunction::serialize(serializer& sink) const
{
  n_ary_operator::serialize(sink);
}

void conjunction::deserialize(deserializer& source)
{
  n_ary_operator::deserialize(source);
}


disjunction* disjunction::clone() const
{
  return new disjunction{*this};
}

bool disjunction::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  return n_ary_operator::equals(other);
}

bool disjunction::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();
  auto& that = static_cast<n_ary_operator const&>(other);
  return std::lexicographical_compare(
      operands.begin(), operands.end(),
      that.operands.begin(), that.operands.end(),
      [](std::unique_ptr<node> const& x, std::unique_ptr<node> const& y)
      {
        return *x < *y;
      });
}

void disjunction::serialize(serializer& sink) const
{
  n_ary_operator::serialize(sink);
}

void disjunction::deserialize(deserializer& source)
{
  n_ary_operator::deserialize(source);
}


ast::ast(std::string const& str, schema const& sch)
  : node_{create(str, sch)}
{
}

ast::ast(node const& n)
  : ast{std::unique_ptr<node>{n.clone()}}
{
}

ast::ast(std::unique_ptr<node> n)
  : node_{std::move(n)}
{
}

ast::ast(ast const& other)
  : node_{other.node_ ? other.node_->clone() : nullptr}
{
}

ast& ast::operator=(ast const& other)
{
  node_.reset(other.node_ ? other.node_->clone() : nullptr);
  return *this;
}

ast& ast::operator=(ast&& other)
{
  node_ = std::move(other.node_);
  return *this;
}

ast::operator bool() const
{
  return node_ != nullptr;
}

void ast::accept(const_visitor& v)
{
  if (node_)
    node_->accept(v);
}

void ast::accept(const_visitor& v) const
{
  if (node_)
    node_->accept(v);
}

node const* ast::root() const
{
  return node_ ? node_.get() : nullptr;
}

void ast::serialize(serializer& sink) const
{
  if (node_)
  {
    sink << true;
    sink << node_;
  }
  else
  {
    sink << false;
  }
}

void ast::deserialize(deserializer& source)
{
  bool valid;
  source >> valid;
  if (valid)
    source >> node_;
}

bool ast::convert(std::string& str, bool tree) const
{
  if (node_)
    return expr::convert(*node_, str, tree);
  str = "";
  return true;
}

bool operator==(ast const& x, ast const& y)
{
  return x.node_ && y.node_ ? *x.node_ == *y.node_ : false;
}

bool operator<(ast const& x, ast const& y)
{
  return x.node_ && y.node_ ? *x.node_ < *y.node_ : false;
}


namespace {

/// Takes a query AST and generates a polymorphic query expression tree.
class expressionizer
{
public:
  using result_type = void;

  expressionizer(n_ary_operator* parent, schema const& sch)
    : parent_(parent),
      schema_(sch)
  {
  }

  void operator()(detail::ast::query::clause const& operand)
  {
    boost::apply_visitor(*this, operand);
  }

  void operator()(detail::ast::query::tag_clause const& clause)
  {
    auto op = clause.op;
    if (invert_)
    {
      op = negate(op);
      invert_ = false;
    }
    auto rel = make_unique<relation>(op);
    std::unique_ptr<extractor> lhs;
    if (clause.lhs == "name")
      lhs = make_unique<name_extractor>();
    else if (clause.lhs == "time")
      lhs = make_unique<timestamp_extractor>();
    else if (clause.lhs == "id")
      lhs = make_unique<id_extractor>();
    auto rhs = make_unique<constant>(detail::ast::query::fold(clause.rhs));
    rel->add(std::move(lhs));
    rel->add(std::move(rhs));
    parent_->add(std::move(rel));
  }

  void operator()(detail::ast::query::type_clause const& clause)
  {
    auto op = clause.op;
    if (invert_)
    {
      op = negate(op);
      invert_ = false;
    }
    auto rel = make_unique<relation>(op);
    auto lhs = make_unique<type_extractor>(clause.lhs);
    auto rhs = make_unique<constant>(detail::ast::query::fold(clause.rhs));
    rel->add(std::move(lhs));
    rel->add(std::move(rhs));
    parent_->add(std::move(rel));
  }

  void operator()(detail::ast::query::offset_clause const& clause)
  {
    auto op = clause.op;
    if (invert_)
    {
      op = negate(op);
      invert_ = false;
    }
    auto rel = make_unique<relation>(op);
    auto lhs = make_unique<offset_extractor>(clause.off);
    auto rhs = make_unique<constant>(detail::ast::query::fold(clause.rhs));
    rel->add(std::move(lhs));
    rel->add(std::move(rhs));
    parent_->add(std::move(rel));
  }

  void operator()(detail::ast::query::event_clause const& clause)
  {
    auto op = clause.op;
    if (invert_)
    {
      op = negate(op);
      invert_ = false;
    }
    if (schema_.events().empty())
      throw error::query("no events in schema");
    // An event clause always consists of two components: the event name
    // extractor and offset extractor. For event dereference sequences (e.g.,
    // http_request$c$..) the event name is explict, for type dereference
    // sequences (e.g., connection$id$...), we need to find all events and
    // types that have an argument of the given type.
    schema::record_type const* event = nullptr;
    auto& symbol = clause.lhs.front();
    for (auto& e : schema_.events())
    {
      if (e.name == symbol)
      {
        event = &e;
        break;
      }
    }
    if (event)
    {
      // Ignore the event name in lhs[0].
      auto& ids = clause.lhs;
      auto offs = schema::argument_offsets(event, {ids.begin() + 1, ids.end()});
      if (offs.empty())
        throw error::schema("unknown argument name");
      // TODO: factor rest of block in separate function to promote DRY.
      auto rel = make_unique<relation>(op);
      auto lhs = make_offset_extractor(std::move(offs));
      auto rhs = make_constant(clause.rhs);
      rel->add(std::move(lhs));
      rel->add(std::move(rhs));
      conjunction* conj;
      if (! (conj = dynamic_cast<conjunction*>(parent_)))
      {
        auto c = make_unique<conjunction>();
        conj = c.get();
        parent_->add(std::move(c));
      }
      conj->add(make_glob_node(symbol));
      conj->add(std::move(rel));
    }
    else
    {
      // The first element in the dereference sequence names a type, now we
      // have to identify all events and records having argument of that
      // type.
      auto found = false;
      for (auto& t : schema_.types())
      {
        if (symbol == t.name)
        {
          found = true;
          break;
        }
      }
      if (! found)
        throw error::query("lhs[0] of clause names neither event nor type");
      for (auto& e : schema_.events())
      {
        auto offsets = schema::symbol_offsets(&e, clause.lhs);
        if (offsets.empty())
          continue;
        // TODO: factor rest of block in separate function to promote DRY.
        if (offsets.size() > 1)
          throw error::schema("multiple offsets not yet implemented");
        auto rel = make_unique<relation>(op);
        auto lhs = make_offset_extractor(std::move(offsets[0]));
        auto rhs = make_constant(clause.rhs);
        rel->add(std::move(lhs));
        rel->add(std::move(rhs));
        conjunction* conj;
        if (! (conj = dynamic_cast<conjunction*>(parent_)))
        {
          auto c = make_unique<conjunction>();
          conj = c.get();
          parent_->add(std::move(c));
        }
        conj->add(make_glob_node(e.name));
        conj->add(std::move(rel));
      }
    }
  }

  void operator()(detail::ast::query::negated_clause const& clause)
  {
    // Since all operators have a complement, we can push down the negation to
    // the operator-level (as opposed to leaving it at the clause level).
    invert_ = true;
    boost::apply_visitor(*this, clause.operand);
  }

private:
  std::unique_ptr<offset_extractor>
  make_offset_extractor(offset o)
  {
    auto node = make_unique<offset_extractor>(std::move(o));
    return std::move(node);
  }

  std::unique_ptr<constant>
  make_constant(detail::ast::query::expression const& expr)
  {
    return make_unique<constant>(detail::ast::query::fold(expr));
  }

  std::unique_ptr<node> make_glob_node(std::string const& expr)
  {
    // Determine whether we need a regular expression node or whether basic
    // equality comparison suffices. This check is relatively crude at the
    // moment: we just look whether the expression contains * or ?.
    auto glob = regex("\\*|\\?").search(expr);
    auto rel = make_unique<relation>(glob ? match : equal);
    auto lhs = make_unique<name_extractor>();
    rel->add(std::move(lhs));
    if (glob)
      rel->add(make_unique<constant>(regex::glob(expr)));
    else
      rel->add(make_unique<constant>(expr));
    return std::move(rel);
  }

  n_ary_operator* parent_;
  schema const& schema_;
  bool invert_ = false;
};

} // namespace <anonymous>

std::unique_ptr<node> create(std::string const& str, schema const& sch)
{
  if (str.empty())
    return {};

  auto i = str.begin();
  auto end = str.end();
  using iterator = std::string::const_iterator;
  detail::parser::error_handler<iterator> on_error{i, end};
  detail::parser::query<iterator> grammar{on_error};
  detail::parser::skipper<iterator> skipper;
  detail::ast::query::query ast;
  bool success = phrase_parse(i, end, grammar, skipper, ast);
  if (! success || i != end)
    return {};
  if (! detail::ast::query::validate(ast))
    // TODO: propagate error to user.
    return {};
  if (ast.rest.empty())
  {
    // WLOG, we can always add a conjunction as root.
    auto conj = make_unique<conjunction>();
    expressionizer visitor{conj.get(), sch};
    boost::apply_visitor(std::ref(visitor), ast.first);
    return std::move(conj);
  }
  // First, split the query expression at each OR node.
  std::vector<detail::ast::query::query> ors;
  ors.push_back({ast.first, {}});
  for (auto& clause : ast.rest)
    if (clause.op == logical_or)
      ors.push_back({clause.operand, {}});
    else
      ors.back().rest.push_back(clause);
  // Our AST root will be a disjunction iff we have at least two terms.
  std::unique_ptr<n_ary_operator> root;
  if (ors.size() >= 2)
    root = make_unique<disjunction>();
  // Then create a conjunction for each set of subsequent AND nodes between
  // two OR nodes.
  std::unique_ptr<conjunction> conj;
  for (auto& ands : ors)
  {
    n_ary_operator* local_root;
    if (! root)
    {
      root = make_unique<conjunction>();
      local_root = root.get();
    }
    else if (! ands.rest.empty())
    {
      auto conj = make_unique<conjunction>();
      local_root = conj.get();
      root->add(std::move(conj));
    }
    else
    {
      local_root = root.get();
    }
    expressionizer visitor{local_root, sch};
    boost::apply_visitor(std::ref(visitor), ands.first);
    for (auto clause : ands.rest)
      boost::apply_visitor(std::ref(visitor), clause.operand);
  }
  return std::move(root);
}

namespace {

class evaluator : public const_visitor
{
public:
  evaluator(event const& e)
    : event_{e}
  {
  }

  value const& result() const
  {
    return result_;
  }

  virtual void visit(constant const& c)
  {
    result_ = c.val;
  }

  virtual void visit(timestamp_extractor const&)
  {
    result_ = event_.timestamp();
  }

  virtual void visit(name_extractor const&)
  {
    result_ = event_.name();
  }

  virtual void visit(id_extractor const&)
  {
    result_ = event_.id();
  }

  virtual void visit(offset_extractor const& o)
  {
    auto v = event_.at(o.off);
    result_ = v ? *v : invalid;
  }

  virtual void visit(type_extractor const& t)
  {
    if (! extractor_state_)
    {
      extractor_state_ = extractor_state{};
      extractor_state_->pos.emplace_back(&event_, 0);
    }
    result_ = invalid;
    while (! extractor_state_->pos.empty())
    {
      auto& rec = *extractor_state_->pos.back().first;
      auto& idx = extractor_state_->pos.back().second;
      if (idx == rec.size())
      {
        // Out of bounds.
        extractor_state_->pos.pop_back();
        continue;
      }
      auto& arg = rec[idx++];
      if (extractor_state_->pos.size() == 1 && idx == rec.size())
        extractor_state_->complete = true; // Finished with top-most record.
      if (! arg)
        continue;
      if (arg.which() == record_type)
      {
        extractor_state_->pos.emplace_back(&arg.get<record>(), 0);
        continue;
      }
      if (arg.which() == t.type)
      {
        result_ = arg;
        break;
      }
    }
  }

  virtual void visit(relation const& r)
  {
    bool p = false;
    do
    {
      r.operands[0]->accept(*this);
      auto lhs = result_;
      r.operands[1]->accept(*this);
      auto& rhs = result_;
      p = r.predicate(lhs, rhs);
      if (p)
        break;
    }
    while (extractor_state_ && ! extractor_state_->complete);
    if (extractor_state_)
      extractor_state_ = {};
    result_ = p;
  }

  virtual void visit(conjunction const& c)
  {
    result_ = std::all_of(
        c.operands.begin(),
        c.operands.end(),
        [&](std::unique_ptr<node> const& operand) -> bool
        {
          operand->accept(*this);
          assert(result_ && result_.which() == bool_type);
          return result_.get<bool>();
        });
  }

  virtual void visit(disjunction const& d)
  {
    result_ = std::any_of(
        d.operands.begin(),
        d.operands.end(),
        [&](std::unique_ptr<node> const& operand) -> bool
        {
          operand->accept(*this);
          assert(result_ && result_.which() == bool_type);
          return result_.get<bool>();
        });
  }

private:
  struct extractor_state
  {
    std::vector<std::pair<record const*, size_t>> pos;
    bool complete = false; // Flag that indicates whether the type extractor
                           // has gone through all values with the given type.
  };

  event const& event_;
  value result_;
  optional<extractor_state> extractor_state_;
};

} // namespace <anonymous>

value evaluate(node const& n, event const& e)
{
  evaluator v{e};
  n.accept(v);
  return v.result();
}

value evaluate(ast const& a, event const& e)
{
  return a.root() ? evaluate(*a.root(), e) : invalid;
}

namespace {

class tree_printer : public const_visitor
{
public:
  tree_printer(std::string& str)
    : str_(str)
  {
  }

  virtual void visit(constant const& c)
  {
    indent();
    str_ += to<std::string>(c.val) + '\n';
  }

  virtual void visit(timestamp_extractor const&)
  {
    indent();
    str_ += "&time\n";
  }

  virtual void visit(name_extractor const&)
  {
    indent();
    str_ += "&name\n";
  }

  virtual void visit(id_extractor const&)
  {
    indent();
    str_ += "&id\n";
  }

  virtual void visit(offset_extractor const& o)
  {
    indent();
    str_ += '@';
    auto first = o.off.begin();
    auto last = o.off.end();
    while (first != last)
    {
      str_ += to<std::string>(*first);
      if (++first != last)
        str_ += ",";
    }
    str_ += '\n';
  }

  virtual void visit(type_extractor const& t)
  {
    indent();
    str_ += "type(";
    str_ += to<std::string>(t.type);
    str_ += ")\n";
  }

  virtual void visit(relation const& rel)
  {
    indent();
    switch (rel.op)
    {
      default:
        assert(! "invalid operator type");
        break;
      case match:
        str_ += "~";
        break;
      case not_match:
        str_ += "!~";
        break;
      case in:
        str_ += "in";
        break;
      case not_in:
        str_ += "!in";
        break;
      case equal:
        str_ += "==";
        break;
      case not_equal:
        str_ += "!=";
        break;
      case less:
        str_ += "<";
        break;
      case less_equal:
        str_ += "<=";
        break;
      case greater:
        str_ += ">";
        break;
      case greater_equal:
        str_ += ">=";
        break;
    }
    str_ += '\n';

    ++depth_;
    assert(rel.operands.size() == 2);
    rel.operands[0]->accept(*this);
    rel.operands[1]->accept(*this);
    --depth_;
  }

  virtual void visit(conjunction const& conj)
  {
    indent();
    str_ += "&&\n";
    ++depth_;
    for (auto& op : conj.operands)
      op->accept(*this);
    --depth_;
  }

  virtual void visit(disjunction const& disj)
  {
    indent();
    str_ += "||\n";
    ++depth_;
    for (auto& op : disj.operands)
      op->accept(*this);
    --depth_;
  }

private:
  void indent()
  {
    str_ += std::string(depth_ * 2, indent_);
  }

  unsigned depth_ = 0;
  char indent_ = ' ';
  std::string& str_;
};

class expr_printer : public const_visitor
{
public:
  expr_printer(std::string& str)
    : str_(str)
  {
  }

  virtual void visit(constant const& c)
  {
    str_ += to<std::string>(c.val);
  }

  virtual void visit(timestamp_extractor const&)
  {
    str_ += "&time";
  }

  virtual void visit(name_extractor const&)
  {
    str_ += "&name";
  }

  virtual void visit(id_extractor const&)
  {
    str_ += "&id";
  }

  virtual void visit(offset_extractor const& o)
  {
    str_ += to<std::string>(o.off);
  }

  virtual void visit(type_extractor const& t)
  {
    str_ += ':' + to<std::string>(t.type);
  }

  virtual void visit(relation const& rel)
  {
    rel.operands[0]->accept(*this);
    str_ += ' ';
    switch (rel.op)
    {
      default:
        assert(! "invalid operator type");
        break;
      case match:
        str_ += "~";
        break;
      case not_match:
        str_ += "!~";
        break;
      case in:
        str_ += "in";
        break;
      case not_in:
        str_ += "!in";
        break;
      case equal:
        str_ += "==";
        break;
      case not_equal:
        str_ += "!=";
        break;
      case less:
        str_ += "<";
        break;
      case less_equal:
        str_ += "<=";
        break;
      case greater:
        str_ += ">";
        break;
      case greater_equal:
        str_ += ">=";
        break;
    }
    str_ += ' ';
    rel.operands[1]->accept(*this);
  }

  virtual void visit(conjunction const& conj)
  {
    auto singular = conj.operands.size() == 1;
    if (singular)
      str_ += '{';
    for (size_t i = 0; i < conj.operands.size(); ++i)
    {
      conj.operands[i]->accept(*this);
      if (i + 1 != conj.operands.size())
        str_ += " && ";
    }
    if (singular)
      str_ += '}';
  }

  virtual void visit(disjunction const& disj)
  {
    auto singular = disj.operands.size() == 1;
    if (singular)
      str_ += '[';
    for (size_t i = 0; i < disj.operands.size(); ++i)
    {
      disj.operands[i]->accept(*this);
      if (i + 1 != disj.operands.size())
        str_ += " || ";
    }
    if (singular)
      str_ += ']';
  }

private:
  std::string& str_;
};
} // namespace <anonymous>

bool convert(node const& n, std::string& str, bool tree)
{
  str.clear();
  if (tree)
  {
    tree_printer v{str};
    n.accept(v);
  }
  else
  {
    expr_printer v{str};
    n.accept(v);
  }
  return true;
}

} // namespace expr
} // namespace vast
