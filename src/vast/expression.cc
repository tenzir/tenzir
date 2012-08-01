#include "vast/expression.h"

#include <boost/variant/apply_visitor.hpp>
#include <ze/type/regex.h>
#include <ze/util/make_unique.h>
#include "vast/logger.h"
#include "vast/detail/ast.h"

namespace vast {
namespace expr {

ze::value const& node::result()
{
  if (! ready())
    eval();

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

offset_extractor::offset_extractor(size_t offset)
  : offset_(offset)
{
}

void offset_extractor::eval()
{
  result_ = event_->flat_at(offset_);
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

void conjunction::eval()
{
  ready_ = true;
  result_ = std::all_of(
      operands_.begin(),
      operands_.end(),
      [&](std::unique_ptr<node> const& operand) -> bool
      {
      auto& result = operand->result();
      if (! operand->ready())
      ready_ = false;

      assert(result.which() == ze::bool_type);
      return result.get<bool>();
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
      auto& result = operand->result();
      if (! operand->ready())
      ready_ = false;

      assert(result.which() == ze::bool_type);
      return result.get<bool>();
      });

  if (result_.get<bool>() && ! ready_)
    ready_ = true;
}

relational_operator::relational_operator(detail::ast::clause_operator op)
{
  switch (op)
  {
    default:
      assert(! "invalid operator type");
      break;
    case detail::ast::match:
      op_ = [](ze::value const& lhs, ze::value const& rhs) -> bool
      {
        assert(lhs.which() == ze::string_type);
        assert(rhs.which() == ze::regex_type);
        return rhs.get<ze::regex>().match(lhs.get<ze::string>());
      };
      break;
    case detail::ast::not_match:
      op_ = [](ze::value const& lhs, ze::value const& rhs) -> bool
      {
        assert(lhs.which() == ze::string_type);
        assert(rhs.which() == ze::regex_type);
        return ! rhs.get<ze::regex>().match(lhs.get<ze::string>());
      };
      break;
    case detail::ast::in:
      op_ = [](ze::value const& lhs, ze::value const& rhs) -> bool
      {
        if (lhs.which() == ze::string_type &&
            rhs.which() == ze::regex_type)
          return rhs.get<ze::regex>().search(lhs.get<ze::string>());

        if (lhs.which() == ze::address_type &&
            rhs.which() == ze::prefix_type)
          return rhs.get<ze::prefix>().contains(lhs.get<ze::address>());

        assert(! "operator 'in' not well-defined");
        return false;
      };
      break;
    case detail::ast::not_in:
      op_ = [](ze::value const& lhs, ze::value const& rhs) -> bool
      {
        if (lhs.which() == ze::string_type &&
            rhs.which() == ze::regex_type)
          return ! rhs.get<ze::regex>().search(lhs.get<ze::string>());

        if (lhs.which() == ze::address_type &&
            rhs.which() == ze::prefix_type)
          return ! rhs.get<ze::prefix>().contains(lhs.get<ze::address>());

        assert(! "operator '!in' not well-defined");
        return false;
      };
      break;
    case detail::ast::equal:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs == rhs;
      };
      break;
    case detail::ast::not_equal:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs != rhs;
      };
      break;
    case detail::ast::less:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs < rhs;
      };
      break;
    case detail::ast::less_equal:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs <= rhs;
      };
      break;
    case detail::ast::greater:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs > rhs;
      };
      break;
    case detail::ast::greater_equal:
      op_ = [](ze::value const& lhs, ze::value const& rhs)
      {
        return lhs >= rhs;
      };
      break;
  }
}

void relational_operator::eval()
{
  assert(operands_.size() == 2);

  do
  {
    auto& l = operands_[0]->result();
    do
    {
      auto& r = operands_[1]->result();
      ready_ = op_(l, r);
      if (ready_)
        break;
    }
    while (! operands_[1]->ready());

    if (ready_)
      break;
  }
  while (! operands_[0]->ready());

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

class expressionizer
{
public:
  typedef void result_type;

  expressionizer(expr::n_ary_operator* parent,
                 std::vector<expr::extractor*>& extractors)
    : parent_(parent)
    , extractors_(extractors)
  {
  }

  void operator()(detail::ast::clause const& operand)
  {
    boost::apply_visitor(*this, operand);
  }

  void operator()(detail::ast::tag_clause const& clause)
  {
    auto op = clause.op;
    if (invert_)
    {
      op = detail::ast::negate(op);
      invert_ = false;
    }

    auto relation = std::make_unique<expr::relational_operator>(op);
    auto rhs = std::make_unique<expr::constant>(detail::ast::fold(clause.rhs));
    std::unique_ptr<expr::extractor> lhs;
    if (clause.lhs == "name")
      lhs = std::make_unique<expr::name_extractor>();
    else if (clause.lhs == "time")
      lhs = std::make_unique<expr::timestamp_extractor>();
    else if (clause.lhs == "id")
      lhs = std::make_unique<expr::id_extractor>();

    assert(lhs);
    extractors_.push_back(lhs.get());
    relation->add(std::move(lhs));
    relation->add(std::move(rhs));
    parent_->add(std::move(relation));
  }

  void operator()(detail::ast::type_clause const& clause)
  {
    auto op = clause.op;
    if (invert_)
    {
      op = detail::ast::negate(op);
      invert_ = false;
    }

    auto relation = std::make_unique<expr::relational_operator>(op);
    auto lhs = std::make_unique<expr::exists>(clause.lhs);
    auto rhs = std::make_unique<expr::constant>(detail::ast::fold(clause.rhs));
    extractors_.push_back(lhs.get());
    relation->add(std::move(lhs));
    relation->add(std::move(rhs));
    parent_->add(std::move(relation));
  }

  void operator()(detail::ast::event_clause const& clause)
  {
    // The validation step of the query AST left the first element
    // untouched, as the name extractor uses it. Since all remaining
    // elements used to contain only a sequence of dereference operations
    // that yield a single offset, they are at this point condensed into
    // one element representing this offset.
    assert(clause.lhs.size() == 2);

    // TODO: Factor out this common optimization step of adding nodes directly
    // at the parent of they are conjunctions. We should create a visitor
    // interface and then an optimizer-visitor that performs such steps.
    expr::n_ary_operator* p;
    if (dynamic_cast<expr::conjunction*>(parent_))
    {
      p = parent_;
    }
    else
    {
      auto conj = std::make_unique<expr::conjunction>();
      p = conj.get();
      parent_->add(std::move(conj));
    }

    p->add(make_glob_node(clause.lhs[0]));

    auto op = clause.op;
    if (invert_)
    {
      op = detail::ast::negate(op);
      invert_ = false;
    }

    auto relation = std::make_unique<expr::relational_operator>(op);

    size_t offset = std::strtoul(clause.lhs[1].data(), nullptr, 10);
    auto lhs = std::make_unique<expr::offset_extractor>(offset);
    extractors_.push_back(lhs.get());
    relation->add(std::move(lhs));

    auto rhs = std::make_unique<expr::constant>(detail::ast::fold(clause.rhs));
    relation->add(std::move(rhs));

    p->add(std::move(relation));
  }

  void operator()(detail::ast::negated_clause const& clause)
  {
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

    auto op = std::make_unique<expr::relational_operator>(
        glob ? detail::ast::match : detail::ast::equal);

    auto lhs = std::make_unique<expr::name_extractor>();
    extractors_.push_back(lhs.get());
    op->add(std::move(lhs));
    if (glob)
      op->add(std::make_unique<expr::constant>(ze::regex::glob(expr)));
    else
      op->add(std::make_unique<expr::constant>(expr));

    return std::move(op);
  }

  expr::n_ary_operator* parent_;
  std::vector<expr::extractor*>& extractors_;
  bool invert_ = false;
};

void expression::assign(detail::ast::query const& query)
{
  if (query.rest.empty())
  {
    /// WLOG, we can always add a conjunction as root.
    auto conjunction = std::make_unique<expr::conjunction>();
    expressionizer visitor(conjunction.get(), extractors_);
    boost::apply_visitor(std::ref(visitor), query.first);
    root_ = std::move(conjunction);
  }
  else
  {
    // First, split the query expression at each OR node.
    std::vector<detail::ast::query> ors{detail::ast::query{query.first}};
    for (auto& clause : query.rest)
      if (clause.op == detail::ast::logical_or)
        ors.emplace_back(detail::ast::query{clause.operand});
      else
        ors.back().rest.push_back(clause);

    // Then create a conjunction for each set of subsequent AND nodes between
    // two OR nodes.
    auto disjunction = std::make_unique<expr::disjunction>();
    for (auto& ands : ors)
      if (ands.rest.empty())
      {
        expressionizer visitor(disjunction.get(), extractors_);
        boost::apply_visitor(std::ref(visitor), ands.first);
      }
      else
      {
        auto conjunction = std::make_unique<expr::conjunction>();
        expressionizer visitor(conjunction.get(), extractors_);
        boost::apply_visitor(std::ref(visitor), ands.first);
        for (auto clause : ands.rest)
        {
          assert(clause.op == detail::ast::logical_and);
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

  ze::value r(false);
  while (! root_->ready())
    r = root_->result();

  assert(r.which() == ze::bool_type);

  root_->reset();
  return r.get<bool>();
}

} // namespace vast
