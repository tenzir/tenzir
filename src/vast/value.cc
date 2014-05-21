#include "vast/value.h"

#include <cstring>
#include <iterator>
#include <sstream>
#include <type_traits>
#include "vast/logger.h"
#include "vast/serialization.h"

namespace vast {

value::value(value_invalid)
{
}

value::value(type_tag t)
{
  data_.type(t);
}

value::value(bool b)
{
  data_.bool_ = b;
  data_.type(bool_value);
  data_.engage();
}

value::value(int i)
{
  data_.int_ = i;
  data_.type(int_value);
  data_.engage();
}

value::value(unsigned int i)
{
  data_.uint_ = i;
  data_.type(uint_value);
  data_.engage();
}

value::value(long l)
{
  data_.int_ = l;
  data_.type(int_value);
  data_.engage();
}

value::value(unsigned long l)
{
  data_.uint_ = l;
  data_.type(uint_value);
  data_.engage();
}

value::value(long long ll)
{
  data_.int_ = ll;
  data_.type(int_value);
  data_.engage();
}

value::value(unsigned long long ll)
{
  data_.uint_ = ll;
  data_.type(uint_value);
  data_.engage();
}

value::value(double d)
{
  data_.double_ = d;
  data_.type(double_value);
  data_.engage();
}

value::value(time_range r)
{
  new (&data_.time_range_) time_range(r);
  data_.type(time_range_value);
  data_.engage();
}

value::value(time_point t)
{
  new (&data_.time_point_) time_point(t);
  data_.type(time_point_value);
  data_.engage();
}

value::value(char c)
  : value(&c, 1)
{
}

value::value(char const* s)
  : value(s, std::strlen(s))
{
}

value::value(char const* s, size_t n)
{
  new (&data_.string_) string{s, s + n};
  data_.type(string_value);
  data_.engage();
}

value::value(std::string const& s)
  : value(s.data(), s.size())
{
}

value::value(string s)
{
  new (&data_.string_) string{std::move(s)};
  data_.type(string_value);
  data_.engage();
}

value::value(regex r)
{
  new (&data_.regex_) std::unique_ptr<regex>{new regex{std::move(r)}};
  data_.type(regex_value);
  data_.engage();
}

value::value(address a)
{
  new (&data_.address_) address{a};
  data_.type(address_value);
  data_.engage();
}

value::value(prefix p)
{
  new (&data_.prefix_) prefix{p};
  data_.type(prefix_value);
  data_.engage();
}

value::value(port p)
{
  new (&data_.port_) port{p};
  data_.type(port_value);
  data_.engage();
}

value::value(record r)
{
  new (&data_.record_) std::unique_ptr<record>{new record(std::move(r))};
  data_.type(record_value);
  data_.engage();
}

value::value(vector v)
{
  new (&data_.vector_) std::unique_ptr<vector>{new vector(std::move(v))};
  data_.type(vector_value);
  data_.engage();
}

value::value(set s)
{
  new (&data_.set_) std::unique_ptr<set>{new set(std::move(s))};
  data_.type(set_value);
  data_.engage();
}

value::value(table t)
{
  new (&data_.table_) std::unique_ptr<table>{new table(std::move(t))};
  data_.type(table_value);
  data_.engage();
}

value::operator bool() const
{
  return data_.engaged();
}

bool value::nil() const
{
  return which() != invalid_value && ! data_.engaged();
}

bool value::invalid() const
{
  return which() == invalid_value;
}

type_tag value::which() const
{
  return data_.type();
}

void value::clear()
{
  if (which() == invalid_value)
    return;
  auto t = data_.type();
  data_ = data{};
  data_.type(t);
}

void value::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << data_;
}

void value::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> data_;
  VAST_LEAVE(VAST_THIS);
}

value::data::data()
  : string_()
{
}

value::data::data(data const& other)
  : data()
{
  if (! other.engaged())
  {
    type(other.type());
    return;
  }
  switch (other.type())
  {
    default:
      throw std::runtime_error("corrupt type tag");
      break;
    case invalid_value:
      return;
    case bool_value:
      new (&bool_) bool(other.bool_);
      break;
    case int_value:
      new (&int_) int64_t(other.int_);
      break;
    case uint_value:
      new (&uint_) uint64_t(other.uint_);
      break;
    case double_value:
      new (&double_) double(other.double_);
      break;
    case time_range_value:
      new (&time_range_) time_range{other.time_range_};
      break;
    case time_point_value:
      new (&time_point_) time_point{other.time_point_};
      break;
    case string_value:
      new (&string_) string{other.string_};
      return; // The string tag already contains the type information.
    case regex_value:
      new (&regex_) std::unique_ptr<regex>{new regex{*other.regex_}};
      break;
    case address_value:
      new (&address_) address{other.address_};
      break;
    case prefix_value:
      new (&prefix_) prefix{other.prefix_};
      break;
    case port_value:
      new (&port_) port{other.port_};
      break;
    case record_value:
      new (&record_) std::unique_ptr<record>{new record(*other.record_)};
      break;
    case vector_value:
      new (&vector_) std::unique_ptr<vector>{new vector(*other.vector_)};
      break;
    case set_value:
      new (&set_) std::unique_ptr<set>{new set(*other.set_)};
      break;
    case table_value:
      new (&table_) std::unique_ptr<table>{new table(*other.table_)};
      break;
  }
  type(other.type());
  engage();
}

value::data::data(data&& other)
  : string_{std::move(other.string_)}
{
}

value::data::~data()
{
  switch (type())
  {
    default:
      break;
    case string_value:
      string_.~string();
      break;
    case regex_value:
      regex_.~unique_ptr<regex>();
      break;
    case record_value:
      record_.~unique_ptr<record>();
      break;
    case table_value:
      table_.~unique_ptr<table>();
      break;
  };
}

value::data& value::data::operator=(data other)
{
  using std::swap;
  swap(string_, other.string_);
  return *this;
}

void value::data::construct(type_tag t)
{
  switch (t)
  {
    default:
      throw std::runtime_error("corrupt type tag");
      break;
    case invalid_value:
      break;
    case bool_value:
      bool_ = false;
      break;
    case int_value:
      int_ = 0ll;
      break;
    case uint_value:
      uint_ = 0ull;
      break;
    case double_value:
      double_ = 0.0;
      break;
    case regex_value:
      new (&regex_) std::unique_ptr<regex>{new regex};
      break;
    case address_value:
      new (&address_) address{};
      break;
    case prefix_value:
      new (&prefix_) prefix{};
      break;
    case port_value:
      new (&port_) port{};
      break;
    case record_value:
      new (&record_) std::unique_ptr<record>{new record};
      break;
    case vector_value:
      new (&vector_) std::unique_ptr<vector>{new vector};
      break;
    case set_value:
      new (&set_) std::unique_ptr<set>{new set};
      break;
    case table_value:
      new (&table_) std::unique_ptr<table>{new table};
      break;
  }
}

type_tag value::data::type() const
{
  return static_cast<type_tag>(string_.tag() & 0x3f);
}

void value::data::type(type_tag i)
{
  string_.tag(i | (string_.tag() & 0x40));
}

void value::data::engage()
{
  string_.tag(string_.tag() | 0x40);
}

bool value::data::engaged() const
{
  return string_.tag() & 0x40;
}

void value::data::serialize(serializer& sink) const
{
  sink << string_.tag();
  if (! engaged())
    return;
  switch (type())
  {
    default:
      throw std::runtime_error("corrupt type tag");
      break;
    case bool_value:
      sink << bool_;
      break;
    case int_value:
      sink << int_;
      break;
    case uint_value:
      sink << uint_;
      break;
    case double_value:
      sink << double_;
      break;
    case time_range_value:
      sink << time_range_;
      break;
    case time_point_value:
      sink << time_point_;
      break;
    case string_value:
      sink << string_;
      break;
    case regex_value:
      sink << *regex_;
      break;
    case address_value:
      sink << address_;
      break;
    case prefix_value:
      sink << prefix_;
      break;
    case port_value:
      sink << port_;
      break;
    case record_value:
      sink << *record_;
      break;
    case vector_value:
      sink << *vector_;
      break;
    case set_value:
      sink << *set_;
      break;
    case table_value:
      sink << *table_;
      break;
  }
}

void value::data::deserialize(deserializer& source)
{
  VAST_ENTER();
  uint8_t tag;
  source >> tag;
  string_.tag(tag);
  if (! engaged())
    return;

  switch (type())
  {
    default:
      throw std::runtime_error("corrupt type tag");
      break;
    case bool_value:
      source >> bool_;
      break;
    case int_value:
      source >> int_;
      break;
    case uint_value:
      source >> uint_;
      break;
    case double_value:
      source >> double_;
      break;
    case time_range_value:
      source >> time_range_;
      break;
    case time_point_value:
      source >> time_point_;
      break;
    case string_value:
      source >> string_;
      break;
    case regex_value:
      {
        regex_ = std::make_unique<regex>();
        source >> *regex_;
      }
      break;
    case address_value:
      source >> address_;
      break;
    case prefix_value:
      source >> prefix_;
      break;
    case port_value:
      source >> port_;
      break;
    case record_value:
      {
        record_ = std::make_unique<record>();
        source >> *record_;
      }
      break;
    case vector_value:
      {
        vector_ = std::make_unique<vector>();
        source >> *vector_;
      }
      break;
    case set_value:
      {
        set_ = std::make_unique<set>();
        source >> *set_;
      }
      break;
    case table_value:
      {
        table_ = std::make_unique<table>();
        source >> *table_;
      }
      break;
  }
}

namespace {

template <typename T, typename U>
struct are_comparable :
  std::integral_constant<
  bool,
  std::is_arithmetic<T>::value && std::is_arithmetic<U>::value &&
  ((std::is_signed<T>::value && std::is_signed<U>::value) ||
   (std::is_unsigned<T>::value && std::is_unsigned<U>::value))> { };


#define VAST_DEFINE_BINARY_PREDICATE(name, not_comparable, invalid_comp, op) \
  struct name                                                                \
  {                                                                          \
    typedef bool result_type;                                                \
                                                                             \
    template <typename T, typename U>                                        \
    bool dispatch(T const&, U const&, std::false_type) const                 \
    {                                                                        \
      return not_comparable;                                                 \
    }                                                                        \
                                                                             \
    template <typename T, typename U>                                        \
    bool dispatch(T const& x, U const& y, std::true_type) const              \
    {                                                                        \
      return x op y;                                                         \
    }                                                                        \
                                                                             \
    template <typename T, typename U>                                        \
    bool operator()(T const& x, U const& y) const                            \
    {                                                                        \
      return dispatch(x, y, are_comparable<T, U>());                         \
    }                                                                        \
                                                                             \
    template <typename T>                                                    \
    bool operator()(T const& x, T const& y) const                            \
    {                                                                        \
      return x op y;                                                         \
    }                                                                        \
                                                                             \
    bool operator()(value_invalid, value_invalid) const                      \
    {                                                                        \
      return invalid_comp;                                                   \
    }                                                                        \
  };

//struct value_is_not_equal
//{
//  typedef bool result_type;
//
//  template <typename T, typename U>
//  bool operator()(T const&, U const&) const
//  {
//    return true;
//  }
//
//  bool operator()(value_invalid, value_invalid) const
//  {
//    return false;
//  }
//};

VAST_DEFINE_BINARY_PREDICATE(value_is_equal, false, true, ==)
VAST_DEFINE_BINARY_PREDICATE(value_is_not_equal, true, false, !=)
VAST_DEFINE_BINARY_PREDICATE(value_is_less_than, false, false, <)
VAST_DEFINE_BINARY_PREDICATE(value_is_less_equal, false, false, <=)
VAST_DEFINE_BINARY_PREDICATE(value_is_greater_than, false, false, >)
VAST_DEFINE_BINARY_PREDICATE(value_is_greater_equal, false, false, >=)

#undef VAST_DEFINE_BINARY_PREDICATE

} // namespace <anonymous>

bool operator==(value const& x, value const& y)
{
  return value::visit(x, y, value_is_equal());
}

bool operator<(value const& x, value const& y)
{
  return value::visit(x, y, value_is_less_than());
}

bool operator!=(value const& x, value const& y)
{
  return value::visit(x, y, value_is_not_equal());
}

bool operator>(value const& x, value const& y)
{
  return value::visit(x, y, value_is_greater_than());
}

bool operator<=(value const& x, value const& y)
{
  return value::visit(x, y, value_is_less_equal());
}

bool operator>=(value const& x, value const& y)
{
  return value::visit(x, y, value_is_greater_equal());
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
    if (! (v->which() == record_value && *v))
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
    if (recurse && v.which() == record_value && v)
      v.get<record>().each(f, recurse);
    else
      f(v);
}

bool record::any(std::function<bool(value const&)> f, bool recurse) const
{
  for (auto& v : *this)
  {
    if (recurse && v.which() == record_value)
    {
      if (v && v.get<record>().any(f, true))
        return true;
    }
    else if (f(v))
      return true;
  }
  return false;
}

bool record::all(std::function<bool(value const&)> f, bool recurse) const
{
  for (auto& v : *this)
  {
    if (recurse && v.which() == record_value)
    {
      if (v && ! v.get<record>().all(f, recurse))
        return false;
    }
    else if (! f(v))
      return false;
  }
  return true;
}

void
record::each_offset(std::function<void(value const&, offset const&)> f) const
{
  offset o;
  do_each_offset(f, o);
}

value const* record::do_flat_at(size_t i, size_t& base) const
{
  assert(base <= i);
  for (auto& v : *this)
  {
    if (v.which() == record_value)
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

void record::do_each_offset(std::function<void(value const&, offset const&)> f,
                           offset& o) const
{
  o.push_back(0);
  for (auto& v : *this)
  {
    if (v && v.which() == record_value)
      v.get<record>().do_each_offset(f, o);
    else
      f(v, o);

    ++o.back();
  }

  o.pop_back();
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


void table::each(std::function<void(value const&, value const&)> f) const
{
  for (auto& p : *this)
    f(p.first, p.second);
}

bool table::any(std::function<bool(value const&, value const&)> f) const
{
  for (auto& p : *this)
    if (f(p.first, p.second))
      return true;
  return false;
}

bool table::all(std::function<bool(value const&, value const&)> f) const
{
  for (auto& p : *this)
    if (! f(p.first, p.second))
      return false;
  return true;
}

void table::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << static_cast<super const&>(*this);
}

void table::deserialize(deserializer& source)
{
  VAST_ENTER();
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

} // namespace vast
