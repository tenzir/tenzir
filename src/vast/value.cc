#include "vast/value.h"

#include <cstring>
#include <iterator>
#include <sstream>
#include <type_traits>
#include "vast/container.h"
#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/detail/parser/value.h"
#include "vast/detail/parser/skipper.h"
#include "vast/util/make_unique.h"

namespace vast {

value value::parse(std::string const& str)
{
  value v;
  auto i = str.begin();
  auto end = str.end();
  using iterator = std::string::const_iterator;
  detail::parser::value<iterator> grammar;
  detail::parser::skipper<iterator> skipper;
  bool success = phrase_parse(i, end, grammar, skipper, v);
  if (! success || i != end)
    v = vast::invalid;
  return v;
}

value::value(invalid_value)
{
}

value::value(value_type t)
{
  data_.type(t);
}

value::value(bool b)
{
  data_.bool_ = b;
  data_.type(bool_type);
  data_.engage();
}

value::value(int i)
{
  data_.int_ = i;
  data_.type(int_type);
  data_.engage();
}

value::value(unsigned int i)
{
  data_.uint_ = i;
  data_.type(uint_type);
  data_.engage();
}

value::value(long l)
{
  data_.int_ = l;
  data_.type(int_type);
  data_.engage();
}

value::value(unsigned long l)
{
  data_.uint_ = l;
  data_.type(uint_type);
  data_.engage();
}

value::value(long long ll)
{
  data_.int_ = ll;
  data_.type(int_type);
  data_.engage();
}

value::value(unsigned long long ll)
{
  data_.uint_ = ll;
  data_.type(uint_type);
  data_.engage();
}

value::value(double d)
{
  data_.double_ = d;
  data_.type(double_type);
  data_.engage();
}

value::value(time_range r)
{
  new (&data_.time_range_) time_range(r);
  data_.type(time_range_type);
  data_.engage();
}

value::value(time_point t)
{
  new (&data_.time_point_) time_point(t);
  data_.type(time_point_type);
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
  data_.type(string_type);
  data_.engage();
}

value::value(std::string const& s)
  : value(s.data(), s.size())
{
}

value::value(string s)
{
  new (&data_.string_) string{std::move(s)};
  data_.type(string_type);
  data_.engage();
}

value::value(regex r)
{
  new (&data_.regex_) std::unique_ptr<regex>{new regex{std::move(r)}};
  data_.type(regex_type);
  data_.engage();
}

value::value(address a)
{
  new (&data_.address_) address{a};
  data_.type(address_type);
  data_.engage();
}

value::value(prefix p)
{
  new (&data_.prefix_) prefix{p};
  data_.type(prefix_type);
  data_.engage();
}

value::value(port p)
{
  new (&data_.port_) port{p};
  data_.type(port_type);
  data_.engage();
}

value::value(record r)
{
  new (&data_.record_) std::unique_ptr<record>{new record(std::move(r))};
  data_.type(record_type);
  data_.engage();
}

value::value(vector v)
{
  new (&data_.vector_) std::unique_ptr<vector>{new vector(std::move(v))};
  data_.type(vector_type);
  data_.engage();
}

value::value(set s)
{
  new (&data_.set_) std::unique_ptr<set>{new set(std::move(s))};
  data_.type(set_type);
  data_.engage();
}

value::value(table t)
{
  new (&data_.table_) std::unique_ptr<table>{new table(std::move(t))};
  data_.type(table_type);
  data_.engage();
}

value::value(std::initializer_list<value> list)
{
  new (&data_.record_) std::unique_ptr<record>{new record(std::move(list))};
  data_.type(record_type);
  data_.engage();
}

value::value(std::initializer_list<std::pair<value const, value>> list)
{
  new (&data_.table_) std::unique_ptr<table>{new table(std::move(list))};
  data_.type(table_type);
  data_.engage();
}

value::operator bool() const
{
  return data_.engaged();
}

bool value::nil() const
{
  return which() != invalid_type && ! data_.engaged();
}

bool value::invalid() const
{
  return which() == invalid_type;
}

value_type value::which() const
{
  return data_.type();
}

void value::clear()
{
  if (which() == invalid_type)
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
      throw std::runtime_error("corrupt value type");
      break;
    case invalid_type:
      return;
    case bool_type:
      new (&bool_) bool(other.bool_);
      break;
    case int_type:
      new (&int_) int64_t(other.int_);
      break;
    case uint_type:
      new (&uint_) uint64_t(other.uint_);
      break;
    case double_type:
      new (&double_) double(other.double_);
      break;
    case time_range_type:
      new (&time_range_) time_range{other.time_range_};
      break;
    case time_point_type:
      new (&time_point_) time_point{other.time_point_};
      break;
    case string_type:
      new (&string_) string{other.string_};
      return; // The string tag already contains the type information.
    case regex_type:
      new (&regex_) std::unique_ptr<regex>{new regex{*other.regex_}};
      break;
    case address_type:
      new (&address_) address{other.address_};
      break;
    case prefix_type:
      new (&prefix_) prefix{other.prefix_};
      break;
    case port_type:
      new (&port_) port{other.port_};
      break;
    case record_type:
      new (&record_) std::unique_ptr<record>{new record(*other.record_)};
      break;
    case vector_type:
      new (&vector_) std::unique_ptr<vector>{new vector(*other.vector_)};
      break;
    case set_type:
      new (&set_) std::unique_ptr<set>{new set(*other.set_)};
      break;
    case table_type:
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
    case string_type:
      string_.~string();
      break;
    case regex_type:
      regex_.~unique_ptr<regex>();
      break;
    case record_type:
      record_.~unique_ptr<record>();
      break;
    case table_type:
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

void value::data::construct(value_type t)
{
  switch (t)
  {
    default:
      throw std::runtime_error("corrupt value type");
      break;
    case invalid_type:
      break;
    case bool_type:
      bool_ = false;
      break;
    case int_type:
      int_ = 0ll;
      break;
    case uint_type:
      uint_ = 0ull;
      break;
    case double_type:
      double_ = 0.0;
      break;
    case regex_type:
      new (&regex_) std::unique_ptr<regex>{new regex};
      break;
    case address_type:
      new (&address_) address{};
      break;
    case prefix_type:
      new (&prefix_) prefix{};
      break;
    case port_type:
      new (&port_) port{};
      break;
    case record_type:
      new (&record_) std::unique_ptr<record>{new record};
      break;
    case vector_type:
      new (&vector_) std::unique_ptr<vector>{new vector};
      break;
    case set_type:
      new (&set_) std::unique_ptr<set>{new set};
      break;
    case table_type:
      new (&table_) std::unique_ptr<table>{new table};
      break;
  }
}

value_type value::data::type() const
{
  return static_cast<value_type>(string_.tag() & 0x3f);
}

void value::data::type(value_type i)
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
      throw std::runtime_error("corrupt value type");
      break;
    case bool_type:
      sink << bool_;
      break;
    case int_type:
      sink << int_;
      break;
    case uint_type:
      sink << uint_;
      break;
    case double_type:
      sink << double_;
      break;
    case time_range_type:
      sink << time_range_;
      break;
    case time_point_type:
      sink << time_point_;
      break;
    case string_type:
      sink << string_;
      break;
    case regex_type:
      sink << *regex_;
      break;
    case address_type:
      sink << address_;
      break;
    case prefix_type:
      sink << prefix_;
      break;
    case port_type:
      sink << port_;
      break;
    case record_type:
      sink << *record_;
      break;
    case vector_type:
      sink << *vector_;
      break;
    case set_type:
      sink << *set_;
      break;
    case table_type:
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
      throw std::runtime_error("corrupt value type");
      break;
    case bool_type:
      source >> bool_;
      break;
    case int_type:
      source >> int_;
      break;
    case uint_type:
      source >> uint_;
      break;
    case double_type:
      source >> double_;
      break;
    case time_range_type:
      source >> time_range_;
      break;
    case time_point_type:
      source >> time_point_;
      break;
    case string_type:
      source >> string_;
      break;
    case regex_type:
      {
        regex_ = make_unique<regex>();
        source >> *regex_;
      }
      break;
    case address_type:
      source >> address_;
      break;
    case prefix_type:
      source >> prefix_;
      break;
    case port_type:
      source >> port_;
      break;
    case record_type:
      {
        record_ = make_unique<record>();
        source >> *record_;
      }
      break;
    case vector_type:
      {
        vector_ = make_unique<vector>();
        source >> *vector_;
      }
      break;
    case set_type:
      {
        set_ = make_unique<set>();
        source >> *set_;
      }
      break;
    case table_type:
      {
        table_ = make_unique<table>();
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
    bool operator()(invalid_value, invalid_value) const                      \
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
//  bool operator()(invalid_value, invalid_value) const
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

} // namespace vast
