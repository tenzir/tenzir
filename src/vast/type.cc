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

argument::argument(string name, intrusive_ptr<vast::type> type)
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

trial<offset> record_type::resolve(std::vector<string> const& ids) const
{
  if (ids.empty())
    return error{"empty symbol sequence"};

  offset off;
  auto found = true;
  auto rec = this;
  for (auto id = ids.begin(); id != ids.end() && found; ++id)
  {
    found = false;
    for (size_t i = 0; i < rec->args.size(); ++i)
    {
      if (rec->args[i].name == *id)
      {
        // If the name matches, we have to check whether we're continuing with
        // an intermediate record or have reached the last symbol.
        rec = util::get<record_type>(rec->args[i].type->info());
        if (! (rec || id + 1 == ids.end()))
          return error{"intermediate arguments must be records"};

        off.push_back(i);
        found = true;
        break;
      }
    }
  }

  if (! found)
    return error{"non-existant argument name"};

  return {std::move(off)};
}

namespace {

struct finder
{
  enum mode
  {
    prefix,
    suffix,
    any
  };

  finder(std::vector<string> const& ids, mode m)
    : mode_{m},
      ids_{ids}
  {
    assert(! ids_.empty());
  }

  using result_type = std::vector<offset>;

  template <typename T>
  std::vector<offset> operator()(T const&) const
  {
    std::vector<offset> r;
    if (off_.empty() || ids_.size() > trace_.size())
      return r;

    if (mode_ == prefix)
    {
      for (size_t i = 0; i < ids_.size(); ++i)
        if (ids_[i] != trace_[i])
          return r;
    }
    else if (mode_ == suffix)
    {
      for (size_t i = 0; i < ids_.size(); ++i)
        if (ids_[i] != trace_[i + trace_.size() - ids_.size()])
          return r;
    }
    else
    {
      for (size_t run = 0; run < trace_.size() - ids_.size(); ++run)
      {
        auto found = true;
        for (size_t i = 0; i < ids_.size(); ++i)
          if (ids_[i] != trace_[i + run])
          {
            found = false;
            break;
          }

        if (found)
          break;
      }

      return r;
    }

    r.push_back(off_);
    return r;
  }

  std::vector<offset> operator()(record_type const& r)
  {
    std::vector<offset> offs;

    off_.push_back(0);
    for (auto& arg : r.args)
    {
      trace_.push_back(arg.name);

      for (auto& o : apply_visitor(*this, arg.type->info()))
        offs.push_back(std::move(o));

      trace_.pop_back();
      ++off_.back();
    }

    off_.pop_back();

    return offs;
  }

  mode mode_;
  std::vector<string> const& ids_;
  std::vector<string> trace_;
  offset off_;
};

} // namespace <anonymous>

std::vector<offset>
record_type::find_prefix(std::vector<string> const& ids) const
{
  return finder{ids, finder::prefix}(*this);
}

std::vector<offset>
record_type::find_suffix(std::vector<string> const& ids) const
{
  return finder{ids, finder::suffix}(*this);
}

intrusive_ptr<type> record_type::at(offset const& o) const
{
  auto r = this;
  for (size_t i = 0; i < o.size(); ++i)
  {
    auto& idx = o[i];
    if (idx >= r->args.size())
      return nullptr;

    auto& t = r->args[idx].type;
    if (i + 1 == o.size())
      return t;

    r = util::get<record_type>(t->info());
    if (! r)
      return {};
  }

  return {};
}

bool operator==(record_type const& lhs, record_type const& rhs)
{
  return lhs.args == rhs.args;
}

event_info::event_info(string name, std::vector<argument> args)
  : record_type{std::move(args)},
    name{std::move(name)}
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

intrusive_ptr<type> type::clone() const
{
  return new type{*this};
}

bool type::name(string name)
{
  if (name.empty())
    return false;

  name_ = std::move(name);

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

namespace {

struct compatibility_checker
{
  using result_type = bool;

  template <typename T, typename U>
  bool operator()(T const&, U const&)
  {
    return false;
  }

  template <typename T>
  bool operator()(T const& x, T const& y)
  {
    return x == y;
  }

  bool operator()(record_type const& x, record_type const& y)
  {
    if (x.args.size() != y.args.size())
      return false;

    for (size_t i = 0; i < x.args.size(); ++i)
      if (! apply_visitor_binary(
              *this, x.args[i].type->info(), y.args[i].type->info()))
        return false;

    return true;
  }

  bool operator()(vector_type const& x, vector_type const& y)
  {
    return apply_visitor_binary(
        *this, x.elem_type->info(), y.elem_type->info());
  }

  bool operator()(set_type const& x, set_type const& y)
  {
    return apply_visitor_binary(
        *this, x.elem_type->info(), y.elem_type->info());
  }

  bool operator()(table_type const& x, table_type const& y)
  {
    return
      apply_visitor_binary(*this, x.key_type->info(), y.key_type->info()) &&
      apply_visitor_binary(*this, x.yield_type->info(), y.yield_type->info());
  }
};

} // namespace <anonymous>

bool type::represents(intrusive_ptr<type> const& other) const
{
  assert(other);
  return apply_visitor_binary(compatibility_checker{}, info_, other->info_);
}

value_type type::tag() const
{
  return apply_visitor(value_type_maker{}, info_);
}

bool operator==(type const& lhs, type const& rhs)
{
  return lhs.name_ == rhs.name_ && lhs.info_ == rhs.info_;
}

} // namespace vast
