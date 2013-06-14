#include "vast/value.h"

#include <cstring>
#include <iterator>
#include <sstream>
#include <type_traits>
#include "vast/container.h"
#include "vast/logger.h"
#include "vast/to_string.h"
#include "vast/io/serialization.h"
#include "vast/util/make_unique.h"

namespace vast {

value::value(invalid_value)
  : data_()
{
}

value::value(nil_value)
  : data_()
{
  type(nil_type);
}

value::value(value_type t)
{
  switch (t)
  {
    default:
      throw error::bad_type("corrupt value type", t);
      break;
    case invalid_type:
    case nil_type:
      break;
    case bool_type:
      data_.boolean = false;
      break;
    case int_type:
      data_.integer = 0ll;
      break;
    case uint_type:
      data_.uinteger = 0ull;
      break;
    case double_type:
      data_.dbl = 0.0;
      break;
    case regex_type:
      new (&data_.rx) std::unique_ptr<regex>(new regex);
      break;
    case vector_type:
      new (&data_.vec) std::unique_ptr<vector>(new vector);
      break;
    case set_type:
      new (&data_.st) std::unique_ptr<set>(new set);
      break;
    case table_type:
      new (&data_.tbl) std::unique_ptr<table>(new table);
      break;
    case record_type:
      new (&data_.rec) std::unique_ptr<record>(new record);
      break;
    case address_type:
      new (&data_.addr) address();
      break;
    case prefix_type:
      new (&data_.pfx) prefix();
      break;
    case port_type:
      new (&data_.prt) port();
      break;
  }
  type(t);
}

value::value(bool b)
{
  data_.boolean = b;
  type(bool_type);
}

value::value(int i)
{
  data_.integer = i;
  type(int_type);
}

value::value(unsigned int i)
{
  data_.uinteger = i;
  type(uint_type);
}

value::value(long l)
{
  data_.integer = l;
  type(int_type);
}

value::value(unsigned long l)
{
  data_.uinteger = l;
  type(uint_type);
}

value::value(long long ll)
{
  data_.integer = ll;
  type(int_type);
}

value::value(unsigned long long ll)
{
  data_.uinteger = ll;
  type(uint_type);
}

value::value(double d)
{
  data_.dbl = d;
  type(double_type);
}

value::value(time_range r)
{
  new (&data_.range) time_range(r);
  type(time_range_type);
}

value::value(time_point t)
{
  new (&data_.time) time_point(t);
  type(time_point_type);
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
  new (&data_.str) string(s, s + n);
  type(string_type);
}

value::value(std::string const& s)
  : value(s.data(), s.size())
{
}

value::value(string s)
{
  new (&data_.str) string(std::move(s));
  type(string_type);
}

value::value(regex r)
{
  new (&data_.rx) std::unique_ptr<regex>();
  data_.rx = make_unique<regex>(std::move(r));
  type(regex_type);
}

value::value(vector v)
{
  new (&data_.vec) std::unique_ptr<vector>(new vector(std::move(v)));
  type(vector_type);
}

value::value(set s)
{
  new (&data_.st) std::unique_ptr<set>(new set(std::move(s)));
  type(set_type);
}

value::value(table t)
{
  new (&data_.tbl) std::unique_ptr<table>(new table(std::move(t)));
  type(table_type);
}

value::value(record r)
{
  new (&data_.rec) std::unique_ptr<record>(new record(std::move(r)));
  type(record_type);
}

value::value(value_type t, std::initializer_list<value> list)
{
  switch (t)
  {
    default:
      throw error::bad_type("invalid container type", t);
    case vector_type:
      new (&data_.vec) std::unique_ptr<vector>(new vector(list));
      break;
    case set_type:
      new (&data_.st) std::unique_ptr<set>(new set(list));
      break;
    case table_type:
      new (&data_.tbl) std::unique_ptr<table>(new table(list));
      break;
    case record_type:
      new (&data_.rec) std::unique_ptr<record>(new record(list));
      break;
  }
  type(t);
}

value::value(address a)
{
  new (&data_.addr) address(a);
  type(address_type);
}

value::value(prefix p)
{
  new (&data_.pfx) prefix(p);
  type(prefix_type);
}

value::value(port p)
{
  new (&data_.prt) port(p);
  type(port_type);
}

value::value(value const& other)
  : data_(other.data_)
{
}

value::value(value&& other)
  : data_(std::move(other.data_))
{
}

value& value::operator=(value other)
{
  using std::swap;
  swap(data_, other.data_);
  return *this;
}

void value::clear()
{
  data_ = std::move(data());
}

value_type value::which() const
{
  return type();
}

void value::serialize(io::serializer& sink)
{
  VAST_ENTER(VAST_THIS);
  sink << type();
  switch (type())
  {
    default:
      throw error::bad_type("corrupt value type", type());
      break;
    case invalid_type:
    case nil_type:
      // Do exactly nothing.
      break;
    case bool_type:
      sink << data_.boolean;
      break;
    case int_type:
      sink << data_.integer;
      break;
    case uint_type:
      sink << data_.uinteger;
      break;
    case double_type:
      sink << data_.dbl;
      break;
    case time_range_type:
      sink << data_.range;
      break;
    case time_point_type:
      sink << data_.time;
      break;
    case string_type:
      sink << data_.str;
      break;
    case regex_type:
      sink << *data_.rx;
      break;
    case vector_type:
      sink << *data_.vec;
      break;
    case set_type:
      sink << *data_.st;
      break;
    case table_type:
      sink << *data_.tbl;
      break;
    case record_type:
      sink << *data_.rec;
      break;
    case address_type:
      sink << data_.addr;
      break;
    case prefix_type:
      sink << data_.pfx;
      break;
    case port_type:
      sink << data_.prt;
      break;
  }
}

void value::deserialize(io::deserializer& source)
{
  VAST_ENTER();
  value_type vt;
  source >> vt;
  type(vt);
  switch (type())
  {
    default:
      throw error::bad_type("corrupt value type", type());
      break;
    case invalid_type:
    case nil_type:
      // Do exactly nothing.
      break;
    case bool_type:
      source >> data_.boolean;
      break;
    case int_type:
      source >> data_.integer;
      break;
    case uint_type:
      source >> data_.uinteger;
      break;
    case double_type:
      source >> data_.dbl;
      break;
    case time_range_type:
      source >> data_.range;
      break;
    case time_point_type:
      source >> data_.time;
      break;
    case string_type:
      source >> data_.str;
      break;
    case regex_type:
      {
        data_.rx = make_unique<regex>();
        source >> *data_.rx;
      }
      break;
    case vector_type:
      {
        data_.vec = make_unique<vector>();
        source >> *data_.vec;
      }
      break;
    case set_type:
      {
        data_.st = make_unique<set>();
        source >> *data_.st;
      }
      break;
    case table_type:
      {
        data_.tbl = make_unique<table>();
        source >> *data_.tbl;
      }
      break;
    case record_type:
      {
        data_.rec = make_unique<record>();
        source >> *data_.rec;
      }
      break;
    case address_type:
      source >> data_.addr;
      break;
    case prefix_type:
      source >> data_.pfx;
      break;
    case port_type:
      source >> data_.prt;
      break;
  }
  VAST_LEAVE(VAST_THIS);
}

value_type value::type() const
{
  return static_cast<value_type>(data_.str.tag());
}

void value::type(value_type i)
{
  data_.str.tag(static_cast<char>(i));
}

value::data::data()
  : str()
{
  type(invalid_type);
}

value::data::data(data const& other)
  : str()
{
  switch (other.type())
  {
    default:
      throw error::bad_type("corrupt value type", other.type());
      break;
    case invalid_type:
    case nil_type:
      break;
    case bool_type:
      new (&boolean) bool(other.boolean);
      break;
    case int_type:
      new (&integer) int64_t(other.integer);
      break;
    case uint_type:
      new (&uinteger) uint64_t(other.uinteger);
      break;
    case double_type:
      new (&dbl) double(other.dbl);
      break;
    case time_range_type:
      new (&range) time_range(other.range);
      break;
    case time_point_type:
      new (&time) time_point(other.time);
      break;
    case string_type:
      new (&str) string(other.str);
      break;
    case regex_type:
      new (&rx) std::unique_ptr<regex>(new regex(*other.rx));
      break;
    case vector_type:
      new (&vec) std::unique_ptr<vector>(new vector(*other.vec));
      break;
    case set_type:
      new (&st) std::unique_ptr<set>(new set(*other.st));
      break;
    case table_type:
      new (&tbl) std::unique_ptr<table>(new table(*other.tbl));
      break;
    case record_type:
      new (&rec) std::unique_ptr<record>(new record(*other.rec));
      break;
    case address_type:
      new (&addr) address(other.addr);
      break;
    case prefix_type:
      new (&pfx) prefix(other.pfx);
      break;
    case port_type:
      new (&prt) port(other.prt);
      break;
  }
  type(other.type());
}

value::data::data(data&& other)
  : str(std::move(other.str))
{
}

value::data::~data()
{
  switch (type())
  {
    default:
      break;
    case string_type:
      str.~string();
      break;
    case regex_type:
      rx.~unique_ptr<regex>();
      break;
    case vector_type:
      vec.~unique_ptr<vector>();
      break;
    case set_type:
      st.~unique_ptr<set>();
      break;
    case table_type:
      tbl.~unique_ptr<table>();
      break;
    case record_type:
      rec.~unique_ptr<record>();
      break;
  };
}


value::data& value::data::operator=(data other)
{
  using std::swap;
  swap(str, other.str);
  return *this;
}

value_type value::data::type() const
{
  return static_cast<value_type>(str.tag());
}

void value::data::type(value_type i)
{
  str.tag(static_cast<char>(i));
}

template <typename T, typename U>
struct are_comparable :
  std::integral_constant<
  bool,
  std::is_arithmetic<T>::value && std::is_arithmetic<U>::value &&
  ((std::is_signed<T>::value && std::is_signed<U>::value) ||
   (std::is_unsigned<T>::value && std::is_unsigned<U>::value))> { };


#define VAST_DEFINE_BINARY_PREDICATE(name, invalid_nil, op)               \
  struct name                                                           \
  {                                                                     \
    typedef bool result_type;                                           \
                                                                        \
    template <typename T, typename U>                                   \
    bool dispatch(T const&, U const&, std::false_type) const            \
    {                                                                   \
      return false;                                                     \
    }                                                                   \
                                                                        \
    template <typename T, typename U>                                   \
    bool dispatch(T const& x, U const& y, std::true_type) const         \
    {                                                                   \
      return x op y;                                                    \
    }                                                                   \
                                                                        \
    template <typename T, typename U>                                   \
    bool operator()(T const& x, U const& y) const                       \
    {                                                                   \
      return dispatch(x, y, are_comparable<T, U>());                    \
    }                                                                   \
                                                                        \
    template <typename T>                                               \
    bool operator()(T const& x, T const& y) const                       \
    {                                                                   \
      return x op y;                                                    \
    }                                                                   \
                                                                        \
    bool operator()(invalid_value, invalid_value) const                 \
    {                                                                   \
      return invalid_nil;                                               \
    }                                                                   \
                                                                        \
    bool operator()(nil_value, nil_value) const                         \
    {                                                                   \
      return invalid_nil;                                               \
    }                                                                   \
  }

VAST_DEFINE_BINARY_PREDICATE(value_is_equal, true, ==);
VAST_DEFINE_BINARY_PREDICATE(value_is_not_equal, false, !=);
VAST_DEFINE_BINARY_PREDICATE(value_is_less_than, false, <);
VAST_DEFINE_BINARY_PREDICATE(value_is_less_equal, false, <=);
VAST_DEFINE_BINARY_PREDICATE(value_is_greater_than, false, >);
VAST_DEFINE_BINARY_PREDICATE(value_is_greater_equal, false, >=);

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

namespace {

struct value_to_string
{
  typedef std::string result_type;

  template <typename T>
  std::string operator()(T const& x) const
  {
    return to_string(x);
  }
};

struct value_streamer
{
  typedef void result_type;

  value_streamer(std::ostream& out)
    : out(out)
  {
  }

  void operator()(invalid_value) const
  {
    out << "<invalid>";
  }

  void operator()(nil_value) const
  {
    out << "<nil>";
  }

  template <typename T>
  void operator()(T const& val) const
  {
    out << val;
  }

  void operator()(int64_t i) const
  {
    if (i >= 0)
      out << '+';

    out << i;
  }

  void operator()(double d) const
  {
    char buf[32];
    std::snprintf(buf, 32, "%.10f", d);
    out.write(buf, std::strlen(buf));
  }

  void operator()(bool b) const
  {
    out << (b ? 'T' : 'F');
  }

  std::ostream& out;
};

} // namespace <anonymous>

std::string to_string(value const& v)
{
  std::string str;
  return value::visit(v, value_to_string());
}

std::string to_string(invalid_value /* invalid */)
{
  return "<invalid>";
}

std::string to_string(nil_value /* nil */)
{
  return "<nil>";
}

std::ostream& operator<<(std::ostream& out, value const& v)
{
  value::visit(v, value_streamer(out));
  return out;
}

} // namespace vast
