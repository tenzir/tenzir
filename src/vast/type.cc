#include "vast/type.h"

#include <iostream> // FIXME: remove
#include "vast/regex.h"
#include "vast/serialization.h"
#include "vast/util/convert.h"

namespace vast {

type_const_ptr const type_invalid = type::make<invalid_type>();

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

void enum_type::serialize(serializer& sink) const
{
  sink << fields;
}

void enum_type::deserialize(deserializer& source)
{
  source >> fields;
}

bool operator==(enum_type const& lhs, enum_type const& rhs)
{
  return lhs.fields == rhs.fields;
}

bool operator<(enum_type const& lhs, enum_type const& rhs)
{
  return lhs.fields < rhs.fields;
}

vector_type::vector_type(type_const_ptr elem)
  : elem_type{elem}
{
}

void vector_type::serialize(serializer& sink) const
{
  sink << *elem_type;
}

void vector_type::deserialize(deserializer& source)
{
  auto t = type::make<invalid_type>();
  source >> *t;
  elem_type = t;
}

bool operator==(vector_type const& lhs, vector_type const& rhs)
{
  assert(lhs.elem_type && rhs.elem_type);
  return *lhs.elem_type == *rhs.elem_type;
}

bool operator<(vector_type const& lhs, vector_type const& rhs)
{
  assert(lhs.elem_type && rhs.elem_type);
  return *lhs.elem_type < *rhs.elem_type;
}

set_type::set_type(type_const_ptr elem)
  : elem_type{elem}
{
}

void set_type::serialize(serializer& sink) const
{
  sink << *elem_type;
}

void set_type::deserialize(deserializer& source)
{
  auto t = type::make<invalid_type>();
  source >> *t;
  elem_type = t;
}

bool operator==(set_type const& lhs, set_type const& rhs)
{
  assert(lhs.elem_type && rhs.elem_type);
  return *lhs.elem_type == *rhs.elem_type;
}

bool operator<(set_type const& lhs, set_type const& rhs)
{
  assert(lhs.elem_type && rhs.elem_type);
  return *lhs.elem_type < *rhs.elem_type;
}

table_type::table_type(type_const_ptr key, type_const_ptr yield)
  : key_type{key},
    yield_type{yield}
{
}

void table_type::serialize(serializer& sink) const
{
  sink << *key_type << *yield_type;
}

void table_type::deserialize(deserializer& source)
{
  auto t = type::make<invalid_type>();
  source >> *t;
  key_type = t;

  t = type::make<invalid_type>();
  source >> *t;
  yield_type = t;
}

bool operator==(table_type const& lhs, table_type const& rhs)
{
  assert(lhs.key_type && rhs.key_type);
  assert(lhs.yield_type && rhs.yield_type);

  return *lhs.key_type == *rhs.key_type
      && *lhs.yield_type == *rhs.yield_type;
}

bool operator<(table_type const& lhs, table_type const& rhs)
{
  assert(lhs.key_type && rhs.key_type);
  assert(lhs.yield_type && rhs.yield_type);

  return
    std::tie(*lhs.key_type, *lhs.yield_type) <
    std::tie(*rhs.key_type, *rhs.yield_type);
}

argument::argument(string name, type_const_ptr type)
  : name{std::move(name)},
    type{std::move(type)}
{
}

void argument::serialize(serializer& sink) const
{
  sink << name << *type;
}

void argument::deserialize(deserializer& source)
{
  source >> name;

  auto t = type::make<invalid_type>();
  source >> *t;
  type = t;
}

bool operator==(argument const& lhs, argument const& rhs)
{
  assert(lhs.type && rhs.type);

  return lhs.name == rhs.name && *lhs.type == *rhs.type;
}

bool operator<(argument const& lhs, argument const& rhs)
{
  assert(lhs.type && rhs.type);

  return std::tie(lhs.name, *lhs.type) < std::tie(rhs.name, *rhs.type);
}

record_type::record_type(std::vector<argument> args)
  : args{std::move(args)}
{
}

trial<offset> record_type::resolve(key const& k) const
{
  if (k.empty())
    return error{"empty symbol sequence"};

  offset off;
  auto found = true;
  auto rec = this;
  for (auto id = k.begin(); id != k.end() && found; ++id)
  {
    found = false;
    for (size_t i = 0; i < rec->args.size(); ++i)
    {
      if (rec->args[i].name == *id)
      {
        // If the name matches, we have to check whether we're continuing with
        // an intermediate record or have reached the last symbol.
        rec = util::get<record_type>(rec->args[i].type->info());
        if (! (rec || id + 1 == k.end()))
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
    exact,
    any
  };

  finder(key const& k, mode m, string const& init = "")
    : mode_{m},
      key_{k}
  {
    assert(! key_.empty());

    if (! init.empty())
      trace_.push_back(init);
  }

  using result_type = std::vector<std::pair<offset, key>>;

  template <typename T>
  std::vector<std::pair<offset, key>>
  operator()(T const&) const
  {
    std::vector<std::pair<offset, key>> r;
    if (off_.empty() || key_.size() > trace_.size())
      return r;

    if (mode_ == prefix || mode_ == exact)
    {
      if (mode_ == exact && key_.size() != trace_.size())
        return r;

      for (size_t i = 0; i < key_.size(); ++i)
        if (! match(key_[i], trace_[i]))
          return r;
    }
    else if (mode_ == suffix)
    {
      for (size_t i = 0; i < key_.size(); ++i)
        if (! match(key_[i], trace_[i + trace_.size() - key_.size()]))
          return r;
    }
    else
    {
      for (size_t run = 0; run < trace_.size() - key_.size(); ++run)
      {
        auto found = true;
        for (size_t i = 0; i < key_.size(); ++i)
          if (! match(key_[i], trace_[i + run]))
          {
            found = false;
            break;
          }

        if (found)
          break;
      }

      return r;
    }

    r.emplace_back(off_, trace_);
    return r;
  }

  std::vector<std::pair<offset, key>>
  operator()(record_type const& r)
  {
    std::vector<std::pair<offset, key>> result;

    off_.push_back(0);
    for (auto& arg : r.args)
    {
      trace_.push_back(arg.name);

      for (auto& p : apply_visitor(*this, arg.type->info()))
        result.push_back(std::move(p));

      trace_.pop_back();
      ++off_.back();
    }

    off_.pop_back();

    return result;
  }

  static bool match(string const& key, string const& trace)
  {
    return regex::glob(key.data()).match(trace);
  }

  mode mode_;
  key key_;
  key trace_;
  offset off_;
};

} // namespace <anonymous>

std::vector<std::pair<offset, key>> record_type::find(key const& k) const
{
  return finder{k, finder::exact}(*this);
}

std::vector<std::pair<offset, key>> record_type::find_prefix(key const& k) const
{
  return finder{k, finder::prefix}(*this);
}

std::vector<std::pair<offset, key>> record_type::find_suffix(key const& k) const
{
  return finder{k, finder::suffix}(*this);
}

record_type record_type::flatten() const
{
  record_type result;
  for (auto& outer : args)
    if (auto r = util::get<record_type>(outer.type->info()))
      for (auto& inner : r->flatten().args)
        result.args.emplace_back(outer.name + "." + inner.name, inner.type);
    else
      result.args.push_back(outer);

  return result;
}

record_type record_type::unflatten() const
{
  record_type result;

  for (auto& arg : args)
  {
    auto parts = arg.name.split(".");
    auto leaf = argument{{parts.back().first, parts.back().second}, arg.type};

    record_type* r = &result;
    for (size_t i = 0; i < parts.size() - 1; ++i)
    {
      auto name = string{parts[i].first, parts[i].second};
      if (r->args.empty() || r->args.back().name != name)
        r->args.emplace_back(std::move(name), type::make<record_type>());

      auto next = util::get<record_type>(r->args.back().type->info());

      // Hack: because the traversal algorithm is a lot easier when traversing
      // the symbol sequence front to back, we bypass the immutability of types
      // after construction.
      r = const_cast<record_type*>(next);
    }

    r->args.emplace_back(std::move(leaf));
  }

  return result;
}

type_const_ptr record_type::at(key const& k) const
{
  auto r = this;
  for (size_t i = 0; i < k.size(); ++i)
  {
    auto& id = k[i];
    argument const* arg = nullptr;
    for (auto& a : r->args)
      if (a.name == id)
      {
        arg = &a;
        break;
      }

    if (! arg)
      return {};

    if (i + 1 == k.size())
      return arg->type;

    r = util::get<record_type>(arg->type->info());
    if (! r)
      return {};
  }

  return {};
}

type_const_ptr record_type::at(offset const& o) const
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

void record_type::serialize(serializer& sink) const
{
  sink << args;
}

void record_type::deserialize(deserializer& source)
{
  source >> args;
}

bool operator==(record_type const& lhs, record_type const& rhs)
{
  return lhs.args == rhs.args;
}

bool operator<(record_type const& lhs, record_type const& rhs)
{
  return lhs.args < rhs.args;
}

type::type(type_info ti)
  : info_{std::move(ti)}
{
}

type_ptr type::clone(string name) const
{
  auto t = new type{*this};
  if (! name.empty())
    t->name_ = std::move(name);

  return type_ptr{t};
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

struct eacher
{
  using result_type = void;

  eacher(std::function<void(key const&, offset const&)> f)
    : f_{f},
      key_(1)
  {
  }

  template <typename T>
  void operator()(T const&) const
  {
    f_(key_.back(), off_);
  }

  void operator()(record_type const& r)
  {
    off_.push_back(0);
    for (auto& a : r.args)
    {
      if (a.type->name().empty())
        key_.back().push_back(a.name);
      else
        key_.emplace_back(key{a.type->name()});

      apply_visitor(*this, a.type->info());

      if (a.type->name().empty())
        key_.back().pop_back();
      else
        key_.pop_back();

      ++off_.back();
    }

    off_.pop_back();
  }

  std::function<void(key const&, offset const&)> f_;
  offset off_;
  std::vector<key> key_;
};

} // namespace <anonymous>


type_const_ptr type::at(key const& k) const
{
  if (k.empty())
    return {};
  else if (k.front() != name_)
    return {};
  if (k.size() == 1)
    return shared_from_this();
  else if (auto r = util::get<record_type>(info_))
    return r->at({k.begin() + 1, k.end()});
  else
    return {};
}

type_const_ptr type::at(offset const& o) const
{
  if (auto r = util::get<record_type>(info_))
    return r->at(o);
  else if (o.empty())
    return shared_from_this();
  else
    return {};
}

void type::each(std::function<void(key const&, offset const&)> f) const
{
  if (util::get<record_type>(info_))
    apply_visitor(eacher{f}, info_);
  else
    f({name_}, {});
}

trial<offset> type::cast(key const& k) const
{
  auto t = find(k);
  if (t.empty())
    return error{"no such key: " + to_string(k)};
  else if (t.size() != 1)
    return error{"ambiguous key: " + to_string(k)};
  else
    return std::move(t[0].first);
}

std::vector<std::pair<offset, key>> type::find(key const& k) const
{
  return apply_visitor(finder{k, finder::exact, name_}, info_);
}

std::vector<std::pair<offset, key>> type::find_prefix(key const& k) const
{
  return apply_visitor(finder{k, finder::prefix, name_}, info_);
}

std::vector<std::pair<offset, key>> type::find_suffix(key const& k) const
{
  return apply_visitor(finder{k, finder::suffix, name_}, info_);
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
};

} // namespace <anonymous>

bool type::represents(type_const_ptr const& other) const
{
  if (! other)
    return false;

  return apply_visitor_binary(compatibility_checker{}, info_, other->info_);
}

value_type type::tag() const
{
  return apply_visitor(value_type_maker{}, info_);
}

namespace {

struct type_serializer
{
  type_serializer(serializer& sink)
    : sink_{sink}
  {
  }

  using result_type = void;

  template <typename T>
  void operator()(T const& x)
  {
    sink_ << x;
  }

  serializer& sink_;
};

struct type_deserializer
{
  type_deserializer(deserializer& source)
    : source_{source}
  {
  }

  using result_type = void;

  template <typename T>
  void operator()(T& x)
  {
    source_ >> x;
  }

  deserializer& source_;
};

type_info construct(value_type vt)
{
  switch (vt)
  {
    default:
      return {};
    case invalid_value:
      return invalid_type{};
    case bool_value:
      return bool_type{};
    case int_value:
      return int_type{};
    case uint_value:
      return uint_type{};
    case double_value:
      return double_type{};
    case time_range_value:
      return time_range_type{};
    case time_point_value:
      return time_point_type{};
    case string_value:
      return string_type{};
    case regex_value:
      return regex_type{};
    case address_value:
      return address_type{};
    case prefix_value:
      return prefix_type{};
    case port_value:
      return port_type{};
    case vector_value:
      return vector_type{};
    case set_value:
      return set_type{};
    case table_value:
      return table_type{};
    case record_value:
      return record_type{};
  }
}

} // namespace <anonymous>

void type::serialize(serializer& sink) const
{
  sink << name_;

  sink << tag();
  apply_visitor(type_serializer{sink}, info_);
}

void type::deserialize(deserializer& source)
{
  source >> name_;

  value_type vt;
  source >> vt;
  info_ = construct(vt);
  apply_visitor(type_deserializer{source}, info_);
}

bool operator==(type const& lhs, type const& rhs)
{
  return lhs.name_ == rhs.name_ && lhs.info_ == rhs.info_;
}

namespace {

struct less_than
{
  using result_type = bool;

  template <typename T, typename U>
  bool operator()(T const&, U const&) const
  {
    return false;
  }

  template <typename T>
  bool operator()(T const& x, T const& y) const
  {
    return x < y;
  }
};

} // namespace <anonymous>

bool operator<(type const& lhs, type const& rhs)
{
  return lhs.name_ < rhs.name_
      && apply_visitor_binary(less_than{}, lhs.info_, rhs.info_);
}

} // namespace vast
