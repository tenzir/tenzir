#include "vast/expression.h"

#include <boost/variant/apply_visitor.hpp>
#include <ze/regex.h>
#include <ze/util/make_unique.h>
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/detail/ast/query.h"
#include "vast/detail/parser/parse.h"
#include "vast/detail/parser/query.h"

namespace vast {
namespace expr {

ze::value const& node::result() const
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

void extractor::feed(ze::event const* event)
{
  event_ = event;
  ready_ = false;
}

ze::event const* extractor::event() const
{
  return event_;
}

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

offset_extractor::offset_extractor(std::vector<size_t> offsets)
  : offsets_(std::move(offsets))
{
}

std::vector<size_t> const& offset_extractor::offsets() const
{
  return offsets_;
}

void offset_extractor::eval()
{
  if (event_->empty())
  {
    result_ = ze::invalid;
  }
  else
  {
    ze::record const* record = event_;
    size_t i = 0;
    while (i < offsets_.size() - 1)
    {
      auto off = offsets_[i++];
      if (off >= record->size())
      {
        result_ = ze::invalid;
        break;
      }

      auto& value = (*record)[off];
      if (value.which() != ze::record_type)
      {
        result_ = ze::invalid;
        break;
      }

      record = &value.get<ze::record>();
    }

    auto last = offsets_[i];
    result_ = last < record->size() ? (*record)[last] : ze::invalid;
  }

  ready_ = true;
}

type_extractor::type_extractor(ze::value_type type)
  : type_(type)
{
}

void type_extractor::feed(ze::event const* event)
{
  if (event->empty())
  {
    ready_ = true;
  }
  else
  {
    event_ = event;
    pos_.clear();
    pos_.emplace_back(event, 0);
    ready_ = false;
  }
}

void type_extractor::reset()
{
  pos_.clear();
  result_ = ze::invalid;
  ready_ = false;
}

ze::value_type type_extractor::type() const
{
  return type_;
}

void type_extractor::eval()
{
  while (! pos_.empty())
  {
    auto& record = *pos_.back().first;
    auto& idx = pos_.back().second;
    auto& arg = record[idx];

    if (idx == record.size())
    {
      pos_.pop_back();
      continue;
    }

    ++idx;
    if (arg.which() == ze::record_type)
    {
      pos_.emplace_back(&arg.get<ze::record>(), 0);
    }
    else if (arg.which() == type_)
    {
      result_ = arg;
      if (pos_.size() == 1 && idx == record.size())
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

        assert(operand->result().which() == ze::bool_type);
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

        assert(operand->result().which() == ze::bool_type);
        return operand->result().get<bool>();
      });

  if (result_.get<bool>() && ! ready_)
    ready_ = true;
}

relation::relation(relational_operator op)
  : op_type_(op)
{
  switch (op_type_)
  {
    default:
      assert(! "invalid operator");
      break;
    case match:
      op_ = [](ze::value const& lhs, ze::value const& rhs) -> bool
      {
        if (lhs.which() != ze::string_type || rhs.which() != ze::regex_type)
          return false;

        return rhs.get<ze::regex>().match(lhs.get<ze::string>());
      };
      break;
    case not_match:
      op_ = [](ze::value const& lhs, ze::value const& rhs) -> bool
      {
        if (lhs.which() != ze::string_type || rhs.which() != ze::regex_type)
          return false;

        return ! rhs.get<ze::regex>().match(lhs.get<ze::string>());
      };
      break;
    case in:
      op_ = [](ze::value const& lhs, ze::value const& rhs) -> bool
      {
        if (lhs.which() == ze::string_type &&
            rhs.which() == ze::regex_type)
          return rhs.get<ze::regex>().search(lhs.get<ze::string>());

        if (lhs.which() == ze::address_type &&
            rhs.which() == ze::prefix_type)
          return rhs.get<ze::prefix>().contains(lhs.get<ze::address>());

        return false;
      };
      break;
    case not_in:
      op_ = [](ze::value const& lhs, ze::value const& rhs) -> bool
      {
        if (lhs.which() == ze::string_type &&
            rhs.which() == ze::regex_type)
          return ! rhs.get<ze::regex>().search(lhs.get<ze::string>());

        if (lhs.which() == ze::address_type &&
            rhs.which() == ze::prefix_type)
          return ! rhs.get<ze::prefix>().contains(lhs.get<ze::address>());

        return false;
      };
      break;
    case equal:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs == rhs;
      };
      break;
    case not_equal:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs != rhs;
      };
      break;
    case less:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs < rhs;
      };
      break;
    case less_equal:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs <= rhs;
      };
      break;
    case greater:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs > rhs;
      };
      break;
    case greater_equal:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs >= rhs;
      };
      break;
  }
}

bool relation::test(ze::value const& lhs, ze::value const& rhs) const
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

constant::constant(ze::value value)
{
  result_ = std::move(value);
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
    : parent_(parent)
    , extractors_(extractors)
    , schema_(sch)
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
    auto relation = make_relation(op);

    std::unique_ptr<expr::extractor> lhs;
    if (clause.lhs == "name")
      lhs = std::make_unique<expr::name_extractor>();
    else if (clause.lhs == "time")
      lhs = std::make_unique<expr::timestamp_extractor>();
    else if (clause.lhs == "id")
      lhs = std::make_unique<expr::id_extractor>();
    assert(lhs);
    extractors_.push_back(lhs.get());

    auto rhs = std::make_unique<expr::constant>(
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
    auto relation = make_relation(op);

    auto lhs = std::make_unique<expr::type_extractor>(clause.lhs);
    extractors_.push_back(lhs.get());

    auto rhs = std::make_unique<expr::constant>(
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
    auto relation = make_relation(op);

    auto lhs = std::make_unique<expr::offset_extractor>(clause.offsets);
    extractors_.push_back(lhs.get());

    auto rhs = std::make_unique<expr::constant>(
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
      auto rel = make_relation(op);
      auto lhs = make_offset_extractor(std::move(offs));
      auto rhs = make_constant(clause.rhs);
      rel->add(std::move(lhs));
      rel->add(std::move(rhs));

      expr::conjunction* conj;
      if (! (conj = dynamic_cast<expr::conjunction*>(parent_)))
      {
        auto c = std::make_unique<expr::conjunction>();
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

        auto rel = make_relation(op);
        auto lhs = make_offset_extractor(std::move(offsets[0]));
        auto rhs = make_constant(clause.rhs);
        rel->add(std::move(lhs));
        rel->add(std::move(rhs));

        expr::conjunction* conj;
        if (! (conj = dynamic_cast<expr::conjunction*>(parent_)))
        {
          auto c = std::make_unique<expr::conjunction>();
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
  std::unique_ptr<expr::offset_extractor>
  make_offset_extractor(std::vector<size_t> offsets)
  {
    auto node = std::make_unique<expr::offset_extractor>(std::move(offsets));
    extractors_.push_back(node.get());
    return std::move(node);
  }

  std::unique_ptr<expr::constant>
  make_constant(detail::ast::query::expression const& expr)
  {
    return std::make_unique<expr::constant>(detail::ast::query::fold(expr));
  }

  std::unique_ptr<expr::node> make_glob_node(std::string const& expr)
  {
    // Determine whether we need a regular expression node or whether basic
    // equality comparison suffices. This check is relatively crude at the
    // moment: we just look whether the expression contains * or ?.
    auto glob = ze::regex("\\*|\\?").search(expr);
    auto rel = make_relation(glob ? match : equal);
    auto lhs = std::make_unique<expr::name_extractor>();
    extractors_.push_back(lhs.get());
    rel->add(std::move(lhs));
    if (glob)
      rel->add(std::make_unique<expr::constant>(ze::regex::glob(expr)));
    else
      rel->add(std::make_unique<expr::constant>(expr));

    return std::move(rel);
  }

  std::unique_ptr<expr::relation> make_relation(relational_operator op)
  {
    switch (op)
    {
      default:
        assert(! "missing relational operator in expression");
        return std::unique_ptr<expr::relation>();
      case match:
        return std::make_unique<expr::relation>(match);
      case not_match:
        return std::make_unique<expr::relation>(not_match);
      case in:
        return std::make_unique<expr::relation>(in);
      case not_in:
        return std::make_unique<expr::relation>(not_in);
      case equal:
        return std::make_unique<expr::relation>(equal);
      case not_equal:
        return std::make_unique<expr::relation>(not_equal);
      case less:
        return std::make_unique<expr::relation>(less);
      case less_equal:
        return std::make_unique<expr::relation>(less_equal);
      case greater:
        return std::make_unique<expr::relation>(greater);
      case greater_equal:
        return std::make_unique<expr::relation>(greater_equal);
    }
  }

  expr::n_ary_operator* parent_;
  std::vector<expr::extractor*>& extractors_;
  schema const& schema_;
  bool invert_ = false;
};

expression::expression(expression const& other)
{
  parse(other.str_, other.schema_);
}

expression::expression(expression&& other)
  : str_(std::move(other.str_))
  , schema_(std::move(other.schema_))
  , root_(std::move(other.root_))
  , extractors_(std::move(other.extractors_))
{
}

expression& expression::operator=(expression other)
{
  using std::swap;
  swap(str_, other.str_);
  swap(schema_, other.schema_);
  swap(root_, other.root_);
  swap(extractors_, other.extractors_);
  return *this;
}

void expression::parse(std::string str, schema sch)
{
  if (str.empty())
      throw error::query("empty expression");

  str_ = std::move(str);
  schema_ = std::move(sch);
  extractors_.clear();

  detail::ast::query::query ast;
  if (! detail::parser::parse<detail::parser::query>(str_, ast))
    throw error::query("syntax error", str_);

  if (! detail::ast::query::validate(ast))
    throw error::query("semantic error", str_);

  if (ast.rest.empty())
  {
    /// WLOG, we can always add a conjunction as root.
    auto conjunction = std::make_unique<expr::conjunction>();
    expressionizer visitor(conjunction.get(), extractors_, schema_);
    boost::apply_visitor(std::ref(visitor), ast.first);
    root_ = std::move(conjunction);
  }
  else
  {
    // First, split the query expression at each OR node.
    std::vector<detail::ast::query::query> ors{detail::ast::query::query{ast.first}};
    for (auto& clause : ast.rest)
      if (clause.op == logical_or)
        ors.emplace_back(detail::ast::query::query{clause.operand});
      else
        ors.back().rest.push_back(clause);

    // Then create a conjunction for each set of subsequent AND nodes between
    // two OR nodes.
    auto disjunction = std::make_unique<expr::disjunction>();
    for (auto& ands : ors)
      if (ands.rest.empty())
      {
        expressionizer visitor(disjunction.get(), extractors_, schema_);
        boost::apply_visitor(std::ref(visitor), ands.first);
      }
      else
      {
        auto conjunction = std::make_unique<expr::conjunction>();
        expressionizer visitor(conjunction.get(), extractors_, schema_);
        boost::apply_visitor(std::ref(visitor), ands.first);
        for (auto clause : ands.rest)
        {
          assert(clause.op == logical_and);
          boost::apply_visitor(std::ref(visitor), clause.operand);
        }

        disjunction->add(std::move(conjunction));
      }

    root_ = std::move(disjunction);
  }

  assert(! extractors_.empty());
}

bool expression::eval(ze::event const& event)
{
  for (auto ext : extractors_)
    ext->feed(&event);

  while (! root_->ready())
    root_->eval();

  auto& r = root_->result();
  assert(r.which() == ze::bool_type);
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

bool operator==(expression const& x, expression const& y)
{
  return x.str_ == y.str_ && x.schema_ == y.schema_;
}

} // namespace vast
