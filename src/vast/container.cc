#include "vast/container.h"

#include <algorithm>
#include <cassert>
#include "vast/logger.h"
#include "vast/serialization.h"

namespace vast {
namespace {

struct unary_any
{
  unary_any(std::function<bool(value const&)> f)
    : f(f)
  {
  }

  bool operator()(value const& v) const
  {
    switch (v.which())
    {
      default:
        return f(v);
      case vector_type:
        return v.get<vector>().any(f);
      case set_type:
        return v.get<set>().any(f);
      case table_type:
        return v.get<table>().any(f);
      case record_type:
        return v.get<record>().any(f);
    }
  }

  std::function<bool(value const&)> f;
};

struct unary_all
{
  unary_all(std::function<bool(value const&)> f)
    : f(f)
  {
  }

  bool operator()(value const& v) const
  {
    switch (v.which())
    {
      default:
        return f(v);
      case vector_type:
        return v.get<vector>().all(f);
      case set_type:
        return v.get<set>().all(f);
      case table_type:
        return v.get<table>().all(f);
      case record_type:
        return v.get<record>().all(f);
    }
  }

  std::function<bool(value const&)> f;
};

} // namespace


vector::vector()
  : type_(invalid_type)
{
}

vector::vector(vast::value_type type)
  : type_(type)
{
}

vector::vector(std::initializer_list<value> list)
  : super(list)
  , type_(invalid_type)
{
  if (empty())
    throw error::bad_value("cannot deduce type from empty vector",
                           vector_type);

  for (auto const& x : *this)
    if (x.which() != nil_type)
    {
      if (type_ == invalid_type && x.which() != invalid_type)
        type_ = x.which();

      if (type_ != invalid_type && x.which() != type_)
        throw error::bad_type("multiple vector types",
                              type_, x.which());
    }
}

vector::vector(vector const& other)
  : super(other)
  , type_(other.type_)
{
}

vector::vector(vector&& other)
  : super(std::move(other))
  , type_(other.type_)
{
  other.type_ = invalid_type;
}

vector& vector::operator=(vector other)
{
  using std::swap;
  super::swap(other);
  swap(type_, other.type_);
  return *this;
}

void vector::insert(iterator i, value x)
{
  ensure_type(type_, x);
  super::insert(i, std::move(x));
}

void vector::push_back(value x)
{
  ensure_type(type_, x);
  super::push_back(std::move(x));
}

vast::value_type vector::type() const
{
  return type_;
}

bool vector::any_impl(std::function<bool(value const&)> f) const
{
  return std::any_of(begin(), end(), unary_any(f));
}

bool vector::all_impl(std::function<bool(value const&)> f) const
{
  return std::all_of(begin(), end(), unary_all(f));
}

void vector::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << type_;
  sink << static_cast<super const&>(*this);
}

void vector::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> type_;
  source >> static_cast<super&>(*this);
  VAST_LEAVE(VAST_THIS);
}

bool operator==(vector const& x, vector const& y)
{
  return static_cast<vector::super const&>(x) ==
    static_cast<vector::super const&>(y);
}

bool operator<(vector const& x, vector const& y)
{
  return static_cast<vector::super const&>(x) <
    static_cast<vector::super const&>(y);
}

bool set::compare::operator()(super::value_type const& v1,
                              super::value_type const& v2) const
{
  return v1 < v2;
}

set::compare const set::comp = {};

set::set()
  : type_(invalid_type)
{
}

set::set(vast::value_type type)
  : type_(type)
{
}

set::set(std::initializer_list<value> list)
  : super(list)
  , type_(invalid_type)
{
  if (empty())
    throw error::bad_value("cannot deduce type from empty set", set_type);

  for (auto const& x : *this)
    if (x.which() != nil_type)
    {
      if (type_ == invalid_type && x.which() != invalid_type)
        type_ = x.which();

      if (type_ != invalid_type && x.which() != type_)
        throw error::bad_type("multiple set types", set_type);
    }

  std::sort(begin(), end(), comp);
  if (std::unique(begin(), end()) != end())
    throw error::bad_value("set contains duplicates", set_type);
}

set::set(set const& other)
  : super(other)
  , type_(other.type_)
{
}

set::set(set&& other)
  : super(std::move(other))
  , type_(other.type_)
{
  other.type_ = invalid_type;
}

set& set::operator=(set other)
{
  using std::swap;
  super::swap(other);
  swap(type_, other.type_);
  return *this;
}

bool set::insert(value x)
{
  ensure_type(type_, x);

  auto i = std::lower_bound(begin(), end(), x, comp);
  if (i == end() || comp(x, *i))
  {
    i = super::insert(i, std::move(x));
    return true;
  }

  return false;
}

set::iterator set::find(value const& x)
{
  check_type(type_, x);
  auto i = std::lower_bound(begin(), end(), x, comp);
  return (i != end() && comp(x, *i)) ? end() : i;
}

set::const_iterator set::find(value const& x) const
{
  check_type(type_, x);
  auto i = std::lower_bound(cbegin(), cend(), x, comp);
  return (i != cend() && comp(x, *i)) ? cend() : i;
}

vast::value_type set::type() const
{
  return type_;
}

bool set::any_impl(std::function<bool(value const&)> f) const
{
  return std::any_of(begin(), end(), unary_any(f));
}

bool set::all_impl(std::function<bool(value const&)> f) const
{
  return std::all_of(begin(), end(), unary_all(f));
}

void set::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << type_;
  sink << static_cast<super const&>(*this);
}

void set::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> type_;
  source >> static_cast<super&>(*this);
  VAST_LEAVE(VAST_THIS);
}

bool operator==(set const& x, set const& y)
{
  return static_cast<set::super const&>(x) ==
    static_cast<set::super const&>(y);
}

bool operator<(set const& x, set const& y)
{
  return static_cast<set::super const&>(x) <
    static_cast<set::super const&>(y);
}


bool table::compare::operator()(value const& key1, value const& key2) const
{
  return key1 < key2;
}

bool table::compare::operator()(value_type const& val1,
                                value_type const& val2) const
{
  return val1.first < val2.first;
}

bool table::compare::operator()(key_type const& key,
                                value_type const& val) const
{
  return key < val.first;
}

bool table::compare::operator()(value_type const& val,
                                key_type const& key) const
{
  return val.first < key;
}

table::compare const table::comp = {};

table::table()
  : key_type_(invalid_type)
  , map_type_(invalid_type)
{
}

table::table(vast::value_type key_type, vast::value_type map_type)
  : key_type_(key_type)
  , map_type_(map_type)
{
}

table::table(std::initializer_list<value> list)
  : key_type_(invalid_type)
  , map_type_(invalid_type)
{
  if (list.size() % 2 != 0)
    throw error::bad_value("initializer list has odd length", table_type);

  for (auto i = list.begin(); i != list.end(); i += 2)
    emplace_back(*i, *(i + 1));

  if (empty())
    throw error::bad_value("cannot deduce type from empty table", table_type);

  for (auto const& i : *this)
  {
    if (i.first.which() != nil_type)
    {
      if (key_type_ == invalid_type && i.first.which() != invalid_type)
        key_type_ = i.first.which();

      if (key_type_ != invalid_type && i.first.which() != key_type_)
        throw error::bad_type("multiple key types",
                              key_type_, i.first.which());
    }

    if (i.second.which() != nil_type)
    {
      if (map_type_ == invalid_type && i.second.which() != invalid_type)
        map_type_ = i.second.which();

      if (map_type_ != invalid_type && i.second.which() != map_type_)
        throw error::bad_type("multiple map types",
                              map_type_, i.second.which());
    }
  }

  std::sort(begin(), end(), comp);
  if (std::unique(begin(), end()) != end())
    throw error::bad_value("table contains duplicates", table_type);
}

table::table(table const& other)
  : super(other)
  , key_type_(other.key_type_)
  , map_type_(other.map_type_)
{
}

table::table(table&& other)
  : super(std::move(other))
  , key_type_(other.key_type_)
  , map_type_(other.map_type_)
{
  other.key_type_ = invalid_type;
  other.map_type_ = invalid_type;
}

table& table::operator=(table other)
{
  using std::swap;
  super::swap(other);
  swap(key_type_, other.key_type_);
  swap(map_type_, other.map_type_);
  return *this;
}

vast::value_type table::key_value_type() const
{
  return key_type_;
}

vast::value_type table::map_value_type() const
{
  return map_type_;
}

table::mapped_type& table::operator[](const key_type& key)
{
  auto v = value_type(key, mapped_type(map_type_));
  return insert(std::move(v)).first->second;
}

std::pair<table::iterator, bool> table::insert(value_type v)
{
  ensure_type(key_type_, v.first);
  ensure_type(map_type_, v.second);

  auto i = std::lower_bound(begin(), end(), v, comp);
  if (i == end() || comp(v, *i))
    return std::make_pair(super::insert(i, std::move(v)), true);

  return std::make_pair(i, false);
}

table::iterator table::find(key_type const& key)
{
  check_type(key_type_, key);
  auto i = std::lower_bound(begin(), end(), key, comp);
  return (i != end() && comp(key, *i)) ? end() : i;
}

table::const_iterator table::find(key_type const& key) const
{
  check_type(key_type_, key);
  auto i = std::lower_bound(cbegin(), cend(), key, comp);
  return (i != cend() && comp(key, *i)) ? cend() : i;
}

bool table::any_impl(std::function<bool(value const&)> f) const
{
  return std::any_of(
      begin(),
      end(),
      [&f](value_type const& pair)
      {
        return unary_any(f)(pair.first) || unary_any(f)(pair.second);
      });
}

bool table::all_impl(std::function<bool(value const&)> f) const
{
  return std::all_of(
      begin(),
      end(),
      [&f](value_type const& pair)
      {
        return unary_all(f)(pair.first) && unary_all(f)(pair.second);
      });
}

void table::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << key_type_;
  sink << map_type_;
  sink << static_cast<super const&>(*this);
}

void table::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> key_type_;
  source >> map_type_;
  source >> static_cast<super&>(*this);
  VAST_LEAVE(VAST_THIS);
}

bool operator==(table const& x, table const& y)
{
  return static_cast<table::super const&>(x) ==
    static_cast<table::super const&>(y);
}

bool operator<(table const& x, table const& y)
{
  return static_cast<table::super const&>(x) <
    static_cast<table::super const&>(y);
}


record::record()
{
}

record::record(std::vector<value> values)
  : super(std::move(values))
{
}

record::record(std::initializer_list<value> list)
  : super(list)
{
}

record::record(record const& other)
  : super(other)
{
}

record::record(record&& other)
  : super(std::move(other))
{
}

record& record::operator=(record other)
{
  using std::swap;
  super::swap(other);
  return *this;
}

value const* record::at(offset const& o) const
{
  record const* r = this;
  for (size_t i = 0; i < o.size(); ++i)
  {
    auto& idx = o[i];
    if (idx >= r->size())
      return nullptr;

    auto v = &(*r)[idx];
    if (i + 1 == o.size())
      return v;

    if (v->which() != record_type)
      return nullptr;

    r = &v->get<record>();
  }

  return nullptr;
}

value const* record::flat_at(size_t i) const
{
  size_t base = 0;
  return do_flat_at(i, base);
}

size_t record::flat_size() const
{
  size_t count = 0;
  each([&](value const&) { ++count; }, true);
  return count;
}

void record::each(std::function<void(value const&)> f, bool recurse) const
{
  for (auto& v : *this)
    if (recurse && v.which() == record_type)
      v.get<record>().each(f, true);
    else
      f(v);
}

value const* record::do_flat_at(size_t i, size_t& base) const
{
  assert(base <= i);
  for (auto& v : *this)
  {
    if (v.which() == record_type)
    {
      auto result = v.get<record>().do_flat_at(i, base);
      if (result)
        return result;
    }
    else if (base++ == i)
      return &v;
  }

  return nullptr;
}

bool record::any_impl(std::function<bool(value const&)> f) const
{
  return std::any_of(begin(), end(), unary_any(f));
}

bool record::all_impl(std::function<bool(value const&)> f) const
{
  return std::all_of(begin(), end(), unary_all(f));
}

void record::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << static_cast<super const&>(*this);
}

void record::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> static_cast<super&>(*this);
  VAST_LEAVE(VAST_THIS);
}

bool operator==(record const& x, record const& y)
{
  return static_cast<record::super const&>(x) ==
    static_cast<record::super const&>(y);
}

bool operator<(record const& x, record const& y)
{
  return static_cast<record::super const&>(x) <
    static_cast<record::super const&>(y);
}

} // namespace vast
