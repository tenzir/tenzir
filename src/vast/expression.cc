#include "vast/expression.h"

#include <boost/variant/apply_visitor.hpp>
#include <ze/type/regex.h>
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

exists::exists(ze::value_type type)
  : type_(type)
{
}

void exists::feed(ze::event const* event)
{
  event_ = event;
  flat_size_ = event->flat_size();
  current_ = 0;
  ready_ = false;
}

void exists::reset()
{
  current_ = 0;
  ready_ = false;
}

void exists::eval()
{
  while (current_ < flat_size_)
  {
    auto& arg = event_->flat_at(current_++);
    if (type_ == arg.which())
    {
      result_ = arg;
      if (current_ == flat_size_)
        ready_ = true;

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

relational_operator::relational_operator(relation_type type)
  : type_(type)
{
  switch (type_)
  {
    default:
      assert(! "invalid operator type");
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

bool relational_operator::test(ze::value const& lhs, ze::value const& rhs) const
{
  return op_(lhs, rhs);
}

relation_type relational_operator::type() const
{
  return type_;
}

void relational_operator::eval()
{
  assert(operands_.size() == 2);

  auto& lhs = operands_[0];
  do
  {
    if (! lhs->ready())
      lhs->eval();

    auto& rhs = operands_[1];
    do
    {
      if (! rhs->ready())
        rhs->eval();

      ready_ = op_(lhs->result(), rhs->result());
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
      op = detail::ast::query::negate(op);
      invert_ = false;
    }
    auto relation = make_relational_operator(op);

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
      op = detail::ast::query::negate(op);
      invert_ = false;
    }
    auto relation = make_relational_operator(op);

    auto lhs = std::make_unique<expr::exists>(clause.lhs);
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
      op = detail::ast::query::negate(op);
      invert_ = false;
    }
    auto relation = make_relational_operator(op);

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
    if (schema_.events().empty())
      throw error::query("no events in schema");

    // An event clause always consists of two components: the event name
    // extractor and offset extractor. For event dereference sequence (e.g.,
    // http_request$c$..) the event name is explict, for type dereference
    // sequences (e.g., connection$id$...), we need to find all events and
    // types that have an argument of the given type.
    //
    // In any case, we always need a conjunction that joins the relevant event
    // names with the actual offset extraction.
    auto conjunction = std::make_unique<expr::conjunction>();

    std::vector<size_t> offsets;
    schema::record_type const* current = nullptr;
    auto& id = clause.lhs.front();
    for (auto& e : schema_.events())
    {
      if (id == e.name)
      {
        current = &e;
        conjunction->add(make_glob_node(id));
        break;
      }
    }
    if (! current)
    {
      // The first element in the dereference sequence names a type, now we
      // have to identify all events and records having argument of that
      // type.
      throw error::query("type dereference not yet implemented");
    }

    for (size_t i = 1; i < clause.lhs.size(); ++i)
    {
      auto& id = clause.lhs[i];
      assert(current);
      for (size_t off = 0; off < current->args.size(); ++off)
      {
        auto& arg = current->args[off];
        if (arg.name == id)
        {
          DBG(query)
            << "translating event clause: "
            << (clause.lhs[0] + '$' + id) << " -> " << off;
          offsets.push_back(off);
          break;
        }
      }
    }

    auto op = clause.op;
    if (invert_)
    {
      op = detail::ast::query::negate(op);
      invert_ = false;
    }
    auto relation = make_relational_operator(op);
    auto lhs = std::make_unique<expr::offset_extractor>(std::move(offsets));
    extractors_.push_back(lhs.get());
    auto rhs = std::make_unique<expr::constant>(
        detail::ast::query::fold(clause.rhs));

    relation->add(std::move(lhs));
    relation->add(std::move(rhs));
    conjunction->add(std::move(relation));
    parent_->add(std::move(conjunction));
  }

  void operator()(detail::ast::query::negated_clause const& clause)
  {
    // Since all operators have a complement, we can push down the negation to
    // the operator-level (as opposed to leaving it at the clause level).
    invert_ = true;
    boost::apply_visitor(*this, clause.operand);
  }

private:
  std::unique_ptr<expr::node> make_glob_node(std::string const& expr)
  {
    // Determine whether we need a regular expression node or whether basic
    // equality comparison suffices. This check is relatively crude at the
    // moment: we just look whether the expression contains * or ?.
    auto glob = ze::regex("\\*|\\?").search(expr);

    auto op = make_relational_operator(
        glob ? detail::ast::query::match : detail::ast::query::equal);

    auto lhs = std::make_unique<expr::name_extractor>();
    extractors_.push_back(lhs.get());
    op->add(std::move(lhs));
    if (glob)
      op->add(std::make_unique<expr::constant>(ze::regex::glob(expr)));
    else
      op->add(std::make_unique<expr::constant>(expr));

    return std::move(op);
  }

  std::unique_ptr<expr::relational_operator>
  make_relational_operator(detail::ast::query::clause_operator op)
  {
    typedef expr::relation_type type;
    switch (op)
    {
      default:
        assert(! "missing relational operator in expression");
        return std::unique_ptr<expr::relational_operator>();
      case detail::ast::query::match:
        return std::make_unique<expr::relational_operator>(type::match);
      case detail::ast::query::not_match:
        return std::make_unique<expr::relational_operator>(type::not_match);
      case detail::ast::query::in:
        return std::make_unique<expr::relational_operator>(type::in);
      case detail::ast::query::not_in:
        return std::make_unique<expr::relational_operator>(type::not_in);
      case detail::ast::query::equal:
        return std::make_unique<expr::relational_operator>(type::equal);
      case detail::ast::query::not_equal:
        return std::make_unique<expr::relational_operator>(type::not_equal);
      case detail::ast::query::less:
        return std::make_unique<expr::relational_operator>(type::less);
      case detail::ast::query::less_equal:
        return std::make_unique<expr::relational_operator>(type::less_equal);
      case detail::ast::query::greater:
        return std::make_unique<expr::relational_operator>(type::greater);
      case detail::ast::query::greater_equal:
        return std::make_unique<expr::relational_operator>(type::greater_equal);
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
      if (clause.op == detail::ast::query::logical_or)
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
          assert(clause.op == detail::ast::query::logical_and);
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
