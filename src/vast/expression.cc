#include "vast/expression.h"

#include <boost/variant/apply_visitor.hpp>
#include "vast/convert.h"
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/regex.h"
#include "vast/serialization.h"
#include "vast/detail/ast/query.h"
#include "vast/detail/parser/error_handler.h"
#include "vast/detail/parser/skipper.h"
#include "vast/detail/parser/query.h"
#include "vast/util/make_unique.h"

namespace vast {
namespace expr {

value const& node::result() const
{
  return result_;
}

bool node::ready() const
{
  return ready_;
}

void node::reset()
{
  ready_ = false;
}

void extractor::feed(event const* e)
{
  event_ = e;
  ready_ = false;
}

//event const* extractor::event() const
//{
//  return event_;
//}

void timestamp_extractor::eval()
{
  result_ = event_->timestamp();
  ready_ = true;
}

void name_extractor::eval()
{
  result_ = event_->name();
  ready_ = true;
}

void id_extractor::eval()
{
  result_ = event_->id();
  ready_ = true;
}

offset_extractor::offset_extractor(offset o)
  : offset_{std::move(o)}
{
}

offset const& offset_extractor::off() const
{
  return offset_;
}

void offset_extractor::eval()
{
  if (event_->empty())
  {
    result_ = invalid;
  }
  else
  {
    record const* rec = event_;
    size_t i = 0;
    while (i < offset_.size() - 1)
    {
      auto off = offset_[i++];
      if (off >= rec->size())
      {
        result_ = invalid;
        break;
      }

      auto& val = (*rec)[off];
      if (val.which() != record_type)
      {
        result_ = invalid;
        break;
      }

      rec = &val.get<record>();
    }

    auto last = offset_[i];
    result_ = last < rec->size() ? (*rec)[last] : invalid;
  }

  ready_ = true;
}

type_extractor::type_extractor(value_type type)
  : type_{type}
{
}

void type_extractor::feed(event const* e)
{
  if (e->empty())
  {
    ready_ = true;
  }
  else
  {
    event_ = e;
    pos_.clear();
    pos_.emplace_back(e, 0);
    ready_ = false;
  }
}

void type_extractor::reset()
{
  pos_.clear();
  result_ = invalid;
  ready_ = false;
}

value_type type_extractor::type() const
{
  return type_;
}

void type_extractor::eval()
{
  while (! pos_.empty())
  {
    auto& rec = *pos_.back().first;
    auto& idx = pos_.back().second;
    auto& arg = rec[idx];

    if (idx == rec.size())
    {
      pos_.pop_back();
      continue;
    }

    ++idx;
    if (arg.which() == record_type)
    {
      pos_.emplace_back(&arg.get<record>(), 0);
    }
    else if (arg.which() == type_)
    {
      result_ = arg;
      if (pos_.size() == 1 && idx == rec.size())
      {
        pos_.clear();
        ready_ = true;
      }

      return;
    }
  }

  ready_ = true;
}

void n_ary_operator::add(std::unique_ptr<node> operand)
{
  operands_.push_back(std::move(operand));
}

void n_ary_operator::reset()
{
  for (auto& op : operands_)
    op->reset();

  ready_ = false;
}

std::vector<std::unique_ptr<node>>& n_ary_operator::operands()
{
  return operands_;
}

std::vector<std::unique_ptr<node>> const& n_ary_operator::operands() const
{
  return operands_;
}

void conjunction::eval()
{
  ready_ = true;
  result_ = std::all_of(
      operands_.begin(),
      operands_.end(),
      [&](std::unique_ptr<node> const& operand) -> bool
      {
        if (! operand->ready())
          operand->eval();
        if (! operand->ready())
          ready_ = false;

        assert(operand->result().which() == bool_type);
        return operand->result().get<bool>();
      });
}

void disjunction::eval()
{
  ready_ = true;
  result_ = std::any_of(
      operands_.begin(),
      operands_.end(),
      [&](std::unique_ptr<node> const& operand) -> bool
      {
        if (! operand->ready())
          operand->eval();
        if (! operand->ready())
          ready_ = false;

        assert(operand->result().which() == bool_type);
        return operand->result().get<bool>();
      });

  if (result_.get<bool>() && ! ready_)
    ready_ = true;
}

relation::relation(relational_operator op)
  : op_type_{op}
{
  switch (op_type_)
  {
    default:
      assert(! "invalid operator");
      break;
    case match:
      op_ = [](value const& lhs, value const& rhs) -> bool
      {
        if (lhs.which() != string_type || rhs.which() != regex_type)
          return false;

        return rhs.get<regex>().match(lhs.get<string>());
      };
      break;
    case not_match:
      op_ = [](value const& lhs, value const& rhs) -> bool
      {
        if (lhs.which() != string_type || rhs.which() != regex_type)
          return false;

        return ! rhs.get<regex>().match(lhs.get<string>());
      };
      break;
    case in:
      op_ = [](value const& lhs, value const& rhs) -> bool
      {
        if (lhs.which() == string_type &&
            rhs.which() == regex_type)
          return rhs.get<regex>().search(lhs.get<string>());

        if (lhs.which() == address_type &&
            rhs.which() == prefix_type)
          return rhs.get<prefix>().contains(lhs.get<address>());

        return false;
      };
      break;
    case not_in:
      op_ = [](value const& lhs, value const& rhs) -> bool
      {
        if (lhs.which() == string_type &&
            rhs.which() == regex_type)
          return ! rhs.get<regex>().search(lhs.get<string>());

        if (lhs.which() == address_type &&
            rhs.which() == prefix_type)
          return ! rhs.get<prefix>().contains(lhs.get<address>());

        return false;
      };
      break;
    case equal:
      op_ = [](value const& lhs, value const& rhs)
      {
        return lhs == rhs;
      };
      break;
    case not_equal:
      op_ = [](value const& lhs, value const& rhs)
      {
        return lhs != rhs;
      };
      break;
    case less:
      op_ = [](value const& lhs, value const& rhs)
      {
        return lhs < rhs;
      };
      break;
    case less_equal:
      op_ = [](value const& lhs, value const& rhs)
      {
        return lhs <= rhs;
      };
      break;
    case greater:
      op_ = [](value const& lhs, value const& rhs)
      {
        return lhs > rhs;
      };
      break;
    case greater_equal:
      op_ = [](value const& lhs, value const& rhs)
      {
        return lhs >= rhs;
      };
      break;
  }
}

bool relation::test(value const& lhs, value const& rhs) const
{
  return op_(lhs, rhs);
}

relational_operator relation::type() const
{
  return op_type_;
}

void relation::eval()
{
  assert(operands_.size() == 2);
  auto& lhs = operands_[0];
  auto& rhs = operands_[1];
  do
  {
    if (! lhs->ready())
      lhs->eval();

    do
    {
      if (! rhs->ready())
        rhs->eval();

      ready_ = test(lhs->result(), rhs->result());
      if (ready_)
        break;
    }
    while (! rhs->ready());

    if (ready_)
      break;
  }
  while (! lhs->ready());

  result_ = ready_;
  ready_ = true;
}

constant::constant(value val)
{
  result_ = std::move(val);
  ready_ = true;
}

void constant::reset()
{
  // Do exactly nothing.
}

void constant::eval()
{
  // Do exactly nothing.
}

} // namespace expr

/// Takes a query AST and generates a polymorphic query expression tree.
class expressionizer
{
public:
  typedef void result_type;

  expressionizer(expr::n_ary_operator* parent,
                 std::vector<expr::extractor*>& extractors,
                 schema const& sch)
    : parent_(parent),
      extractors_(extractors),
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
    auto relation = make_unique<expr::relation>(op);

    std::unique_ptr<expr::extractor> lhs;
    if (clause.lhs == "name")
      lhs = make_unique<expr::name_extractor>();
    else if (clause.lhs == "time")
      lhs = make_unique<expr::timestamp_extractor>();
    else if (clause.lhs == "id")
      lhs = make_unique<expr::id_extractor>();
    assert(lhs);
    extractors_.push_back(lhs.get());

    auto rhs = make_unique<expr::constant>(
        detail::ast::query::fold(clause.rhs));

    relation->add(std::move(lhs));
    relation->add(std::move(rhs));
    parent_->add(std::move(relation));
  }

  void operator()(detail::ast::query::type_clause const& clause)
  {
    auto op = clause.op;
    if (invert_)
    {
      op = negate(op);
      invert_ = false;
    }
    auto relation = make_unique<expr::relation>(op);

    auto lhs = make_unique<expr::type_extractor>(clause.lhs);
    extractors_.push_back(lhs.get());

    auto rhs = make_unique<expr::constant>(
        detail::ast::query::fold(clause.rhs));

    relation->add(std::move(lhs));
    relation->add(std::move(rhs));
    parent_->add(std::move(relation));
  }

  void operator()(detail::ast::query::offset_clause const& clause)
  {
    auto op = clause.op;
    if (invert_)
    {
      op = negate(op);
      invert_ = false;
    }
    auto relation = make_unique<expr::relation>(op);

    auto lhs = make_unique<expr::offset_extractor>(clause.off);
    extractors_.push_back(lhs.get());

    auto rhs = make_unique<expr::constant>(
        detail::ast::query::fold(clause.rhs));

    relation->add(std::move(lhs));
    relation->add(std::move(rhs));
    parent_->add(std::move(relation));
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
      auto relation = make_unique<expr::relation>(op);
      auto lhs = make_offset_extractor(std::move(offs));
      auto rhs = make_constant(clause.rhs);
      relation->add(std::move(lhs));
      relation->add(std::move(rhs));

      expr::conjunction* conj;
      if (! (conj = dynamic_cast<expr::conjunction*>(parent_)))
      {
        auto c = make_unique<expr::conjunction>();
        conj = c.get();
        parent_->add(std::move(c));
      }
      conj->add(make_glob_node(symbol));
      conj->add(std::move(relation));
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

        auto relation = make_unique<expr::relation>(op);
        auto lhs = make_offset_extractor(std::move(offsets[0]));
        auto rhs = make_constant(clause.rhs);
        relation->add(std::move(lhs));
        relation->add(std::move(rhs));

        expr::conjunction* conj;
        if (! (conj = dynamic_cast<expr::conjunction*>(parent_)))
        {
          auto c = make_unique<expr::conjunction>();
          conj = c.get();
          parent_->add(std::move(c));
        }
        conj->add(make_glob_node(e.name));
        conj->add(std::move(relation));
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
  std::unique_ptr<expr::offset_extractor>
  make_offset_extractor(offset o)
  {
    auto node = make_unique<expr::offset_extractor>(std::move(o));
    extractors_.push_back(node.get());
    return std::move(node);
  }

  std::unique_ptr<expr::constant>
  make_constant(detail::ast::query::expression const& expr)
  {
    return make_unique<expr::constant>(detail::ast::query::fold(expr));
  }

  std::unique_ptr<expr::node> make_glob_node(std::string const& expr)
  {
    // Determine whether we need a regular expression node or whether basic
    // equality comparison suffices. This check is relatively crude at the
    // moment: we just look whether the expression contains * or ?.
    auto glob = regex("\\*|\\?").search(expr);
    auto relation = make_unique<expr::relation>(glob ? match : equal);
    auto lhs = make_unique<expr::name_extractor>();
    extractors_.push_back(lhs.get());
    relation->add(std::move(lhs));
    if (glob)
      relation->add(make_unique<expr::constant>(regex::glob(expr)));
    else
      relation->add(make_unique<expr::constant>(expr));
    return std::move(relation);
  }

  std::unique_ptr<expr::relation> make_relation(relational_operator op)
  {
    return make_unique<expr::relation>(op);
  }

  expr::n_ary_operator* parent_;
  std::vector<expr::extractor*>& extractors_;
  schema const& schema_;
  bool invert_ = false;
};

expression expression::parse(std::string const& str, schema sch)
{
  expression e;
  if (str.empty())
      throw error::query("empty expression");

  e.str_ = std::move(str);
  e.schema_ = std::move(sch);
  e.extractors_.clear();

  auto i = str.begin();
  auto end = str.end();
  using iterator = std::string::const_iterator;
  detail::parser::error_handler<iterator> on_error{i, end};
  detail::parser::query<iterator> grammar{on_error};
  detail::parser::skipper<iterator> skipper;
  detail::ast::query::query ast;
  bool success = phrase_parse(i, end, grammar, skipper, ast);
  if (! success || i != end)
    throw error::query("syntax error", e.str_);

  if (! detail::ast::query::validate(ast))
    throw error::query("semantic error", e.str_);

  if (ast.rest.empty())
  {
    /// WLOG, we can always add a conjunction as root.
    auto conjunction = make_unique<expr::conjunction>();
    expressionizer visitor{conjunction.get(), e.extractors_, e.schema_};
    boost::apply_visitor(std::ref(visitor), ast.first);
    e.root_ = std::move(conjunction);
  }
  else
  {
    // First, split the query expression at each OR node.
    std::vector<detail::ast::query::query> ors;
    ors.emplace_back(detail::ast::query::query{ast.first, {}});
    for (auto& clause : ast.rest)
      if (clause.op == logical_or)
        ors.emplace_back(detail::ast::query::query{clause.operand, {}});
      else
        ors.back().rest.push_back(clause);

    // Then create a conjunction for each set of subsequent AND nodes between
    // two OR nodes.
    auto disjunction = make_unique<expr::disjunction>();
    for (auto& ands : ors)
      if (ands.rest.empty())
      {
        expressionizer visitor(disjunction.get(), e.extractors_, e.schema_);
        boost::apply_visitor(std::ref(visitor), ands.first);
      }
      else
      {
        auto conjunction = make_unique<expr::conjunction>();
        expressionizer visitor(conjunction.get(), e.extractors_, e.schema_);
        boost::apply_visitor(std::ref(visitor), ands.first);
        for (auto clause : ands.rest)
        {
          assert(clause.op == logical_and);
          boost::apply_visitor(std::ref(visitor), clause.operand);
        }

        disjunction->add(std::move(conjunction));
      }

    e.root_ = std::move(disjunction);
  }
  assert(! e.extractors_.empty());
  return e;
}



expression::expression(expression const& other)
{
  *this = parse(other.str_, other.schema_);
}

expression::expression(expression&& other)
  : str_{std::move(other.str_)},
    schema_{std::move(other.schema_)},
    root_{std::move(other.root_)},
    extractors_{std::move(other.extractors_)}
{
}

bool expression::eval(event const& e)
{
  for (auto ext : extractors_)
    ext->feed(&e);

  while (! root_->ready())
    root_->eval();

  auto& r = root_->result();
  assert(r.which() == bool_type);
  root_->reset();
  return r.get<bool>();
}

void expression::accept(expr::const_visitor& v) const
{
  assert(root_);
  root_->accept(v);
}

void expression::accept(expr::visitor& v)
{
  assert(root_);
  root_->accept(v);
}

void expression::serialize(serializer& sink) const
{
  sink << str_;
  sink << schema_;
}

void expression::deserialize(deserializer& source)
{
  std::string str;
  source >> str;
  schema sch;
  source >> sch;
  *this = parse(std::move(str), std::move(sch));
}

namespace {

class stringifier : public expr::const_visitor
{
public:
  stringifier(std::string& str)
    : str_(str)
  {
  }

  virtual void visit(expr::node const&)
  {
    assert(! "should never happen");
  }

  virtual void visit(expr::timestamp_extractor const&)
  {
    indent();
    str_ += "&time\n";
  }

  virtual void visit(expr::name_extractor const&)
  {
    indent();
    str_ += "&name\n";
  }

  virtual void visit(expr::id_extractor const&)
  {
    indent();
    str_ += "&id\n";
  }

  virtual void visit(expr::offset_extractor const& o)
  {
    indent();
    str_ += '@';
    auto first = o.off().begin();
    auto last = o.off().end();
    while (first != last)
    {
      str_ += to<std::string>(*first);
      if (++first != last)
        str_ += ",";
    }
    str_ += '\n';
  }

  virtual void visit(expr::type_extractor const& e)
  {
    indent();
    str_ += "type(";
    str_ += to<std::string>(e.type());
    str_ += ")\n";
  }

  virtual void visit(expr::conjunction const& conj)
  {
    indent();
    str_ += "&&\n";
    ++depth_;
    for (auto& op : conj.operands())
      op->accept(*this);
    --depth_;
  }

  virtual void visit(expr::disjunction const& disj)
  {
    indent();
    str_ += "||\n";
    ++depth_;
    for (auto& op : disj.operands())
      op->accept(*this);
    --depth_;
  }

  virtual void visit(expr::relation const& rel)
  {
    assert(rel.operands().size() == 2);

    indent();
    switch (rel.type())
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
    rel.operands()[0]->accept(*this);
    rel.operands()[1]->accept(*this);
    --depth_;
  }

  virtual void visit(expr::constant const& c)
  {
    indent();
    str_ += to<std::string>(c.result()) + '\n';
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

} // namespace <anonymous>

bool expression::convert(std::string& str) const
{
  str.clear();
  stringifier visitor(str);
  accept(visitor);
  return true;
}

bool operator==(expression const& x, expression const& y)
{
  return x.str_ == y.str_ && x.schema_ == y.schema_;
}

} // namespace vast
