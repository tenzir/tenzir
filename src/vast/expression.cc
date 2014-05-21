#include "vast/expression.h"

#include <boost/variant/apply_visitor.hpp>
#include "vast/logger.h"
#include "vast/optional.h"
#include "vast/serialization.h"

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

offset_extractor::offset_extractor()
  : type{type_invalid}
{
}

offset_extractor::offset_extractor(type_const_ptr type, offset off)
  : type{type},
    off{std::move(off)}
{
  assert(type);
}

offset_extractor* offset_extractor::clone() const
{
  return new offset_extractor{*this};
}

void offset_extractor::serialize(serializer& sink) const
{
  sink << *type << off;
}

void offset_extractor::deserialize(deserializer& source)
{
  auto t = type::make<invalid_type>();
  source >> *t >> off;
  type = t;
}

bool offset_extractor::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;

  auto& that = static_cast<offset_extractor const&>(other);
  return *type == *that.type && off == that.off;
}

bool offset_extractor::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();

  auto& that = static_cast<offset_extractor const&>(other);
  return std::tie(*type, off) < std::tie(*that.type, that.off);
}


schema_extractor::schema_extractor(vast::key k)
  : key{std::move(k)}
{
}

schema_extractor* schema_extractor::clone() const
{
  return new schema_extractor{*this};
}

bool schema_extractor::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  return key == static_cast<schema_extractor const&>(other).key;
}

bool schema_extractor::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();
  return key < static_cast<schema_extractor const&>(other).key;
}

void schema_extractor::serialize(serializer& sink) const
{
  sink << key;
}

void schema_extractor::deserialize(deserializer& source)
{
  source >> key;
}


type_extractor::type_extractor(type_tag t)
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

namespace {

struct match_visitor
{
  using result_type = bool;

  result_type operator()(string const& lhs, regex const& rhs) const
  {
    return rhs.match(lhs);
  }

  template <typename T, typename U>
  result_type operator()(T const&, U const&) const
  {
    return false;
  }
};

struct in_visitor
{
  using result_type = bool;

  result_type operator()(string const& lhs, string const& rhs) const
  {
    return rhs.find(lhs) != string::npos;
  }

  result_type operator()(string const& lhs, regex const& rhs) const
  {
    return rhs.search(lhs);
  }

  result_type operator()(address const& lhs, prefix const& rhs) const
  {
    return rhs.contains(lhs);
  }

  template <typename T>
  result_type operator()(T const& lhs, set const& rhs) const
  {
    return std::find(rhs.begin(), rhs.end(), value{lhs}) != rhs.end();
  }

  template <typename T>
  result_type operator()(T const& lhs, vector const& rhs) const
  {
    return std::find(rhs.begin(), rhs.end(), value{lhs}) != rhs.end();
  }

  template <typename T, typename U>
  result_type operator()(T const&, U const&) const
  {
    return false;
  }
};

struct ni_visitor
{
  using result_type = bool;

  template <typename T, typename U>
  bool operator()(T const& lhs, U const& rhs) const
  {
    return in_visitor{}(rhs, lhs);
  }
};

} // namespace <anonymous>

predicate::binary_predicate predicate::make_predicate(relational_operator op)
{
  switch (op)
  {
    default:
      assert(! "invalid operator");
      return {};
    case match:
      return [](value const& lhs, value const& rhs) -> bool
      {
        return value::visit(lhs, rhs, match_visitor{});
      };
    case not_match:
      return [](value const& lhs, value const& rhs) -> bool
      {
        return ! value::visit(lhs, rhs, match_visitor{});
      };
    case in:
      return [](value const& lhs, value const& rhs) -> bool
      {
        return value::visit(lhs, rhs, in_visitor{});
      };
    case not_in:
      return [](value const& lhs, value const& rhs) -> bool
      {
        return ! value::visit(lhs, rhs, in_visitor{});
      };
    case ni:
      return [](value const& lhs, value const& rhs) -> bool
      {
        return value::visit(lhs, rhs, ni_visitor{});
      };
    case not_ni:
      return [](value const& lhs, value const& rhs) -> bool
      {
        return ! value::visit(lhs, rhs, ni_visitor{});
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

predicate::predicate(relational_operator op)
  : op{op}
{
  pred = make_predicate(op);
}

node const& predicate::lhs() const
{
  assert(operands.size() == 2);
  assert(operands[0]);
  return *operands[0];
}

node const& predicate::rhs() const
{
  assert(operands.size() == 2);
  assert(operands[1]);
  return *operands[1];
}

predicate* predicate::clone() const
{
  return new predicate{*this};
}

bool predicate::equals(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  return op == static_cast<predicate const&>(other).op
      && n_ary_operator::equals(other);
}

bool predicate::is_less_than(node const& other) const
{
  if (typeid(*this) != typeid(other))
    return typeid(*this).hash_code() < typeid(other).hash_code();
  auto& that = static_cast<predicate const&>(other);
  return op != that.op
    ? op < that.op
    : std::lexicographical_compare(
          operands.begin(), operands.end(),
          that.operands.begin(), that.operands.end(),
          [](std::unique_ptr<node> const& x, std::unique_ptr<node> const& y)
          {
            return *x < *y;
          });
}

void predicate::serialize(serializer& sink) const
{
  n_ary_operator::serialize(sink);
  sink << op;
}

void predicate::deserialize(deserializer& source)
{
  n_ary_operator::deserialize(source);
  source >> op;
  pred = make_predicate(op);
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

namespace {

struct conjunction_tester : public default_const_visitor
{
  virtual void visit(conjunction const&)
  {
    flag_ = true;
  }

  bool flag_ = false;
};

struct disjunction_tester : public default_const_visitor
{
  virtual void visit(disjunction const&)
  {
    flag_ = true;
  }

  bool flag_ = false;
};

struct predicate_tester : public default_const_visitor
{
  virtual void visit(predicate const&)
  {
    flag_ = true;
  }

  bool flag_ = false;
};

class meta_predicate_tester : public default_const_visitor
{
public:
  virtual void visit(predicate const& pred)
  {
    pred.lhs().accept(*this);
  }

  virtual void visit(name_extractor const&)
  {
    flag_ = true;
  }

  virtual void visit(timestamp_extractor const&)
  {
    flag_ = true;
  }

  virtual void visit(id_extractor const&)
  {
    flag_ = true;
  }

  bool flag_ = false;
};

class time_predicate_tester : public default_const_visitor
{
public:
  virtual void visit(predicate const& pred)
  {
    pred.lhs().accept(*this);
  }

  virtual void visit(timestamp_extractor const&)
  {
    flag_ = true;
  }

  bool flag_ = false;
};

class name_predicate_tester : public default_const_visitor
{
public:
  virtual void visit(predicate const& pred)
  {
    pred.lhs().accept(*this);
  }

  virtual void visit(name_extractor const&)
  {
    flag_ = true;
  }

  bool flag_ = false;
};

class constant_finder : public default_const_visitor
{
public:
  virtual void visit(predicate const& pred)
  {
    pred.rhs().accept(*this);
  }

  virtual void visit(constant const& c)
  {
    val_ = &c.val;
  }

  value const* val_ = nullptr;
};

class offset_finder : public default_const_visitor
{
public:
  virtual void visit(predicate const& pred)
  {
    pred.lhs().accept(*this);
  }

  virtual void visit(offset_extractor const& oe)
  {
    off_ = &oe.off;
  }

  offset const* off_ = nullptr;
};

class operator_finder : public default_const_visitor
{
public:
  virtual void visit(predicate const& pred)
  {
    op_ = &pred.op;
  }

  relational_operator const* op_ = nullptr;
};

} // namespace <anonymous>

bool ast::is_conjunction() const
{
  conjunction_tester visitor;
  accept(visitor);
  return visitor.flag_;
}

bool ast::is_disjunction() const
{
  disjunction_tester visitor;
  accept(visitor);
  return visitor.flag_;
}

bool ast::is_predicate() const
{
  predicate_tester visitor;
  accept(visitor);
  return visitor.flag_;
}

bool ast::is_meta_predicate() const
{
  meta_predicate_tester visitor;
  accept(visitor);
  return visitor.flag_;
}

bool ast::is_time_predicate() const
{
  time_predicate_tester visitor;
  accept(visitor);
  return visitor.flag_;
}

bool ast::is_name_predicate() const
{
  name_predicate_tester visitor;
  accept(visitor);
  return visitor.flag_;
}

value const* ast::find_constant() const
{
  constant_finder visitor;
  accept(visitor);
  return visitor.val_;
}

offset const* ast::find_offset() const
{
  offset_finder visitor;
  accept(visitor);
  return visitor.off_;
}

relational_operator const* ast::find_operator() const
{
  operator_finder visitor;
  accept(visitor);
  return visitor.op_;
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

bool operator==(ast const& x, ast const& y)
{
  return x.node_ && y.node_ ? *x.node_ == *y.node_ : false;
}

bool operator<(ast const& x, ast const& y)
{
  return x.node_ && y.node_ ? *x.node_ < *y.node_ : false;
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

  virtual void visit(schema_extractor const&)
  {
    assert(! "must resolve AST before evaluating with schema extractor");
    result_ = invalid;
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
      if (arg.which() == record_value)
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

  virtual void visit(predicate const& p)
  {
    bool result = false;
    do
    {
      p.lhs().accept(*this);
      auto lhs = result_;
      p.rhs().accept(*this);
      auto& rhs = result_;
      result = p.pred(lhs, rhs);
      if (result)
        break;
    }
    while (extractor_state_ && ! extractor_state_->complete);
    if (extractor_state_)
      extractor_state_ = {};
    result_ = result;
  }

  virtual void visit(conjunction const& c)
  {
    result_ = std::all_of(
        c.operands.begin(),
        c.operands.end(),
        [&](std::unique_ptr<node> const& operand) -> bool
        {
          operand->accept(*this);
          assert(result_ && result_.which() == bool_value);
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
          assert(result_ && result_.which() == bool_value);
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

bool compatible(type_tag lhs, type_tag rhs, relational_operator op)
{
  switch (op)
  {
    default:
      return false;
    case match:
    case not_match:
      switch (lhs)
      {
        default:
          return false;
        case string_value:
          return rhs == regex_value;
      }
    case equal:
    case not_equal:
    case less:
    case less_equal:
    case greater:
    case greater_equal:
      return lhs == rhs;
    case in:
    case not_in:
      switch (lhs)
      {
        default:
          return is_container(rhs);
        case string_value:
          return rhs == string_value || is_container(rhs);
        case address_value:
          return rhs == prefix_value || is_container(rhs);
      }
    case ni:
      return compatible(rhs, lhs, in);
    case not_ni:
      return compatible(rhs, lhs, not_in);
  }
}

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
    str_ += to_string(c.val) + '\n';
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
    str_ += to_string(*o.type);
    str_ += '@';
    str_ += to_string(o.off);
    str_ += '\n';
  }

  virtual void visit(schema_extractor const& s)
  {
    indent();
    str_ += to_string(s.key);
    str_ += '\n';
  }

  virtual void visit(type_extractor const& t)
  {
    indent();
    str_ += "type(";
    str_ += to_string(t.type);
    str_ += ")\n";
  }

  virtual void visit(predicate const& p)
  {
    indent();
    switch (p.op)
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
      case ni:
        str_ += "ni";
        break;
      case not_ni:
        str_ += "!ni";
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
    p.lhs().accept(*this);
    p.rhs().accept(*this);
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
    str_ += to_string(c.val);
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
    str_ += to_string(*o.type) + '@' + to_string(o.off);
  }

  virtual void visit(schema_extractor const& s)
  {
    str_ += to_string(s.key);
  }

  virtual void visit(type_extractor const& t)
  {
    str_ += ':' + to_string(t.type);
  }

  virtual void visit(predicate const& p)
  {
    p.lhs().accept(*this);
    str_ += ' ';
    switch (p.op)
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
      case ni:
        str_ += "ni";
        break;
      case not_ni:
        str_ += "!ni";
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
    p.rhs().accept(*this);
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

class predicator : public default_const_visitor
{
public:
  predicator(std::vector<ast>& predicates)
    : predicates_{predicates}
  {
  }

  virtual void visit(conjunction const& conj)
  {
    for (auto& op : conj.operands)
      op->accept(*this);
  }

  virtual void visit(disjunction const& disj)
  {
    for (auto& op : disj.operands)
      op->accept(*this);
  }

  virtual void visit(predicate const& pred)
  {
    predicates_.emplace_back(pred);
  }

private:
  std::vector<ast>& predicates_;
};

struct resolver : default_const_visitor
{
public:
  resolver(schema const& sch)
    : schema_{sch}
  {
  }

  virtual void visit(conjunction const& conj)
  {
    // FIXME: Hack until we've switched to util::variant.
    for (auto& op : const_cast<conjunction&>(conj).operands)
    {
      op->accept(*this);
      if (! error_.msg().empty())
        return;

      if (schema_node_)
        op = std::move(schema_node_);
    }
  }

  virtual void visit(disjunction const& disj)
  {
    // FIXME: Hack until we've switched to util::variant.
    for (auto& op : const_cast<disjunction&>(disj).operands)
    {
      op->accept(*this);
      if (! error_.msg().empty())
        return;

      if (schema_node_)
        op = std::move(schema_node_);
    }
  }

  virtual void visit(predicate const& pred)
  {
    rhs_ = &pred.rhs();
    op_ = pred.op;
    pred.lhs().accept(*this);
  }

  virtual void visit(schema_extractor const& pred)
  {
    auto disj = std::make_unique<disjunction>();
    for (auto& t : schema_)
    {
      auto trace = t->find_suffix(pred.key);
      if (trace.empty())
        continue;

      // Make sure that all found keys resolve to arguments with the same type.
      auto first_type = t->at(trace.front().first);
      for (auto& p : trace)
        if (! p.first.empty())
          if (auto r = util::get<record_type>(t->info()))
            if (! first_type->represents(r->at(p.first)))
            {
              error_ =
                error{"type clash: " +
                      to_string(*t) + " : " + to_string(*t, false) +
                      " <--> " + to_string(*r->at(p.first)) + " : " +
                      to_string(*r->at(p.first), false)};
              return;
            }

      // Add all offsets from the trace to the disjunction, which will
      // eventually replace this node.
      for (auto& p : trace)
      {
        auto pr = std::make_unique<predicate>(op_);
        auto lhs = std::make_unique<offset_extractor>(t, std::move(p.first));
        pr->add(std::move(lhs));
        pr->add(std::unique_ptr<node>{rhs_->clone()});
        disj->add(std::move(pr));
      }
    }

    if (disj->operands.empty())
      error_ = error{"invalid key: " + to_string(pred.key)};
    else if (disj->operands.size() == 1)
      // Small optimization: if there is only one operand in the conjuncation,
      // we can lift it directly.
      schema_node_ = std::move(disj->operands[0]);
    else
      schema_node_ = std::move(disj);
  }

  error error_;
  std::unique_ptr<node> schema_node_;
  relational_operator op_;
  node const* rhs_;
  schema const& schema_;
};

} // namespace <anonymous>

std::vector<ast> ast::predicatize() const
{
  std::vector<ast> predicates;
  predicator p{predicates};
  accept(p);
  return predicates;
}

trial<ast> ast::resolve(schema const& sch) const
{
  auto a = *this;
  resolver r{sch};
  a.accept(r);

  if (! r.error_.msg().empty())
    return std::move(r.error_);
  else
    return std::move(a);
}

trial<void> convert(node const& n, std::string& s, bool tree)
{
  if (tree)
  {
    tree_printer v{s};
    n.accept(v);
  }
  else
  {
    expr_printer v{s};
    n.accept(v);
  }

  return nothing;
}

} // namespace expr
} // namespace vast
