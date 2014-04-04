#include "vast/type.h"

namespace vast {
namespace {

struct value_type_maker
{
  using result_type = value_type;

  template <typename T>
  value_type operator()(T const&) const
  {
    return to_value_type<T>::value;
  }
};

} // namespace <anonymous>

enum_type::enum_type(std::vector<string> fields)
  : fields{std::move(fields)}
{
}

bool operator==(enum_type const& lhs, enum_type const& rhs)
{
  return lhs.fields == rhs.fields;
}

vector_type::vector_type(intrusive_ptr<type> elem)
  : elem_type{elem}
{
}

bool operator==(vector_type const& lhs, vector_type const& rhs)
{
  assert(lhs.elem_type && rhs.elem_type);
  return *lhs.elem_type == *rhs.elem_type;
}

set_type::set_type(intrusive_ptr<type> elem)
  : elem_type{elem}
{
}

bool operator==(set_type const& lhs, set_type const& rhs)
{
  assert(lhs.elem_type && rhs.elem_type);
  return *lhs.elem_type == *rhs.elem_type;
}

table_type::table_type(intrusive_ptr<type> key, intrusive_ptr<type> yield)
  : key_type{key},
    yield_type{yield}
{
}

bool operator==(table_type const& lhs, table_type const& rhs)
{
  assert(lhs.key_type && rhs.key_type);
  assert(lhs.yield_type && rhs.yield_type);

  return *lhs.key_type == *rhs.key_type
      && *lhs.yield_type == *rhs.yield_type;
}

argument::argument(std::string name, intrusive_ptr<vast::type> type)
  : name{std::move(name)},
    type{std::move(type)}
{
}

bool operator==(argument const& lhs, argument const& rhs)
{
  assert(lhs.type && rhs.type);

  return lhs.name == rhs.name && *lhs.type == *rhs.type;
}

record_type::record_type(std::vector<argument> args)
  : args{std::move(args)}
{
}

bool operator==(record_type const& lhs, record_type const& rhs)
{
  return lhs.args == rhs.args;
}

event_info::event_info(std::string name, std::vector<argument> args)
  : name{std::move(name)},
    args{std::move(args)}
{
}

bool operator==(event_info const& lhs, event_info const& rhs)
{
  return lhs.name == rhs.name && lhs.args == rhs.args;
}

type::type(type_info ti)
  : info_{std::move(ti)}
{
}

bool type::name(string name)
{
  if (name.empty())
    return false;

  auto i = std::find(aliases_.begin(), aliases_.end(), name);
  if (i != aliases_.end())
    return false;

  name_ = std::move(name);

  return true;
}

bool type::alias(string name)
{
  if (name.empty() || name_ == name)
    return false;

  auto i = std::find(aliases_.begin(), aliases_.end(), name);
  if (i != aliases_.end())
    return false;

  aliases_.push_back(std::move(name));

  return true;
}

type_info const& type::info() const
{
  return info_;
}

string const& type::name() const
{
  return name_;
}

value_type type::tag() const
{
  return apply_visitor(value_type_maker{}, info_);
}

bool operator==(type const& lhs, type const& rhs)
{
  return lhs.name_ == rhs.name_
      && lhs.aliases_ == rhs.aliases_
      && lhs.info_ == rhs.info_;
}

} // namespace vast
