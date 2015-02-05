#ifndef VAST_DATA_H
#define VAST_DATA_H

#include <regex>
#include <string>
#include <vector>
#include <map>
#include <type_traits>
#include "vast/aliases.h"
#include "vast/address.h"
#include "vast/pattern.h"
#include "vast/subnet.h"
#include "vast/port.h"
#include "vast/none.h"
#include "vast/optional.h"
#include "vast/time.h"
#include "vast/type.h"
#include "vast/util/flat_set.h"
#include "vast/util/meta.h"
#include "vast/util/parse.h"
#include "vast/util/print.h"
#include "vast/util/string.h"

namespace vast {

class data;

class vector : public std::vector<data>
{
public:
  using std::vector<vast::data>::vector;
};

class set : public util::flat_set<data>
{
public:
  using util::flat_set<vast::data>::flat_set;
};

class table : public std::map<data, data>
{
public:
  using std::map<vast::data, vast::data>::map;
};

class record : public std::vector<data>
{
public:
  using std::vector<vast::data>::vector;
  using std::vector<vast::data>::at;

  /// Enables recursive record iteration.
  class each : public util::range_facade<each>
  {
  public:
    struct range_state
    {
      vast::data const& operator*() const;
      util::stack::vector<8, vast::data const*> trace;
      vast::offset offset;
    };

    each(record const& r);

  private:
    friend util::range_facade<each>;

    range_state const& state() const
    {
      return state_;
    }

    bool next();

    range_state state_;
    util::stack::vector<8, record const*> records_;
  };

  /// Retrieves a data at a givene offset.
  /// @param o The offset to look at.
  /// @returns A pointer to the data at *o* or `nullptr` if *o* does not
  ///          resolve.
  vast::data const* at(offset const& o) const;

  /// Unflattens a data sequence according to a given record type.
  /// @param t The type holding the structure for unflattening.
  /// @returns The unflattened record on success.
  trial<record> unflatten(type::record const& t) const;
};

class data
{
public:
  template <typename T>
  using from = std::conditional_t<
      std::is_floating_point<T>::value,
      real,
      std::conditional_t<
        std::is_same<T, boolean>::value,
        boolean,
        std::conditional_t<
          std::is_unsigned<T>::value,
          count,
          std::conditional_t<
            std::is_signed<T>::value,
            integer,
            std::conditional_t<
              std::is_convertible<T, std::string>::value,
              std::string,
              std::conditional_t<
                   std::is_same<T, none>::value
                || std::is_same<T, time_point>::value
                || std::is_same<T, time_duration>::value
                || std::is_same<T, pattern>::value
                || std::is_same<T, address>::value
                || std::is_same<T, subnet>::value
                || std::is_same<T, port>::value
                || std::is_same<T, enumeration>::value
                || std::is_same<T, vector>::value
                || std::is_same<T, set>::value
                || std::is_same<T, table>::value
                || std::is_same<T, record>::value,
                T,
                std::false_type
              >
            >
          >
        >
      >
    >;

  template <typename T>
  using type = from<std::decay_t<T>>;

  template <typename T>
  using is_basic = std::integral_constant<
      bool,
      std::is_same<T, boolean>::value
        || std::is_same<T, integer>::value
        || std::is_same<T, count>::value
        || std::is_same<T, real>::value
        || std::is_same<T, time_point>::value
        //|| std::is_same<T, time_interval>::value
        || std::is_same<T, time_duration>::value
        //|| std::is_same<T, time_period>::value
        || std::is_same<T, std::string>::value
        || std::is_same<T, pattern>::value
        || std::is_same<T, address>::value
        || std::is_same<T, subnet>::value
        || std::is_same<T, port>::value
    >;

  template <typename T>
  using is_container = std::integral_constant<
      bool,
      std::is_same<T, vector>::value
        || std::is_same<T, set>::value
        || std::is_same<T, table>::value
    >;

  enum class tag : uint8_t
  {
    none,
    boolean,
    integer,
    count,
    real,
    time_point,
    //time_interval,
    time_duration,
    //time_period,
    string,
    pattern,
    address,
    subnet,
    port,
    enumeration,
    vector,
    set,
    table,
    record
  };

  using variant_type = util::basic_variant<
    tag,
    none,
    boolean,
    integer,
    count,
    real,
    time_point,
    //time_interval,
    time_duration,
    //time_period,
    std::string,
    pattern,
    address,
    subnet,
    port,
    enumeration,
    vector,
    set,
    table,
    record
  >;

  /// Evaluates a data predicate.
  /// @param lhs The LHS of the predicate.
  /// @param op The relational operator.
  /// @param rhs The RHS of the predicate.
  static bool evaluate(data const& lhs, relational_operator op,
                       data const& rhs);

  /// Default-constructs empty data.
  data(none = nil) {}

  /// Constructs data.
  /// @param x The instance to construct data from.
  template <
    typename T,
    typename = util::disable_if_t<
      util::is_same_or_derived<data, T>::value
      || std::is_same<type<T>, std::false_type>::value
    >
  >
  data(T&& x)
    : data_(type<T>(std::forward<T>(x)))
  {
  }

private:
  variant_type data_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  friend variant_type& expose(data& d);
  friend variant_type const& expose(data const& d);

  friend bool operator==(data const& lhs, data const& rhs);
  friend bool operator!=(data const& lhs, data const& rhs);
  friend bool operator<(data const& lhs, data const& rhs);
  friend bool operator<=(data const& lhs, data const& rhs);
  friend bool operator>=(data const& lhs, data const& rhs);
  friend bool operator>(data const& lhs, data const& rhs);
};

//
// Printable Concept
//

namespace detail {

template <typename Iterator>
struct data_printer
{
  data_printer(Iterator& out)
    : out_{out}
  {
  }

  trial<void> operator()(none const&) const
  {
    return print("<nil>", out_);
  }

  template <typename T>
  trial<void> operator()(T const& x) const
  {
    return print(x, out_);
  }

  trial<void> operator()(std::string const& str) const
  {
    *out_++ = '"';

    auto t = print(util::byte_escape(str), out_);
    if (! t)
      return t.error();

    *out_++ = '"';

    return nothing;
  }

  Iterator& out_;
};

} // namespace detail

template <typename Iterator>
trial<void> print(data const& d, Iterator&& out)
{
  return visit(detail::data_printer<Iterator>(out), d);
}

template <typename Iterator>
trial<void> print(vector const& v, Iterator&& out, char const* delim = ", ")
{
  *out++ = '[';

  auto t = util::print_delimited(delim, v.begin(), v.end(), out);
  if (! t)
    return t.error();

  *out++ = ']';

  return nothing;
}

template <typename Iterator>
trial<void> print(set const& s, Iterator&& out, char const* delim = ", ")
{
  *out++ = '{';

  auto t = util::print_delimited(delim, s.begin(), s.end(), out);
  if (! t)
    return t.error();

  *out++ = '}';

  return nothing;
}

template <typename Iterator>
trial<void> print(table const& tab, Iterator&& out)
{
  *out++ = '{';
  auto first = tab.begin();
  auto last = tab.end();
  while (first != last)
  {
    auto t = print(first->first, out);
    if (! t)
      return t.error();

    t = print(" -> ", out);
    if (! t)
      return t.error();

    t = print(first->second, out);
    if (! t)
      return t.error();

    if (++first != last)
    {
      t = print(", ", out);
      if (! t)
        return t.error();
    }
  }

  *out++ = '}';

  return nothing;
}

template <typename Iterator>
trial<void> print(record const& r, Iterator&& out, char const* delim = ", ")
{
  *out++ = '(';

  auto t = util::print_delimited(delim, r.begin(), r.end(), out);
  if (! t)
    return t.error();

  *out++ = ')';

  return nothing;
}

//
// Parsable Concept
//

template <typename Iterator>
trial<void> parse(vector& v, Iterator& begin, Iterator end,
                  type const& elem_type,
                  std::string const& sep = ", ",
                  std::string const& left = "[",
                  std::string const& right = "]",
                  std::string const& esc = "\\")
{
  if (begin == end)
    return error{"empty iterator range"};

  if (static_cast<size_t>(end - begin) < left.size() + right.size())
    return error{"no boundaries present"};

  if (! util::starts_with(begin, end, left))
    return error{"left boundary does not match"};

  if (! util::ends_with(begin, end, right))
    return error{"right boundary does not match"};

  auto s = util::split(begin + left.size(), end - right.size(), sep, esc);
  for (auto& p : s)
  {
    auto t = parse<data>(p.first, p.second, elem_type);
    if (t)
      v.push_back(std::move(*t));
    else
      return t.error();
  }

  begin = end;
  return nothing;
}

template <typename Iterator>
trial<void> parse(set& s, Iterator& begin, Iterator end,
                  type const& elem_type,
                  std::string const& sep = ", ",
                  std::string const& left = "{",
                  std::string const& right = "}",
                  std::string const& esc = "\\")
{
  if (begin == end)
    return error{"empty iterator range"};

  if (static_cast<size_t>(end - begin) < left.size() + right.size())
    return error{"no boundaries present"};

  if (! util::starts_with(begin, end, left))
    return error{"left boundary does not match"};

  if (! util::ends_with(begin, end, right))
    return error{"right boundary does not match"};

  auto split = util::split(begin + left.size(), end - right.size(), sep, esc);
  for (auto& p : split)
  {
    auto t = parse<data>(p.first, p.second, elem_type);
    if (t)
      s.insert(std::move(*t));
    else
      return t.error();
  }

  begin = end;
  return nothing;
}

template <typename Iterator>
trial<void> parse(record& r, Iterator& begin, Iterator end,
                  type::record const& rec_type,
                  std::string const& sep = ", ",
                  std::string const& left = "(",
                  std::string const& right = ")",
                  std::string const& esc = "\\")
{
  if (begin == end)
    return error{"empty iterator range"};

  if (static_cast<size_t>(end - begin) < left.size() + right.size())
    return error{"no boundaries present"};

  if (! util::starts_with(begin, end, left))
    return error{"left boundary does not match"};

  if (! util::ends_with(begin, end, right))
    return error{"right boundary does not match"};

  auto s = util::split(begin + left.size(), end - right.size(), sep, esc);
  if (s.size() != rec_type.fields().size())
    return error{"number of fields don't match provided type"};

  for (size_t i = 0; i < s.size(); ++i)
  {
    auto t = parse<data>(s[i].first, s[i].second, rec_type.fields()[i].type);
    if (t)
      r.push_back(std::move(*t));
    else
      return t.error();
  }

  begin = end;
  return nothing;
}

namespace detail {

template <typename Iterator>
class data_parser
{
public:
  data_parser(data& d, Iterator& begin, Iterator end,
              std::string const& set_sep,
              std::string const& set_left,
              std::string const& set_right,
              std::string const& vec_sep,
              std::string const& vec_left,
              std::string const& vec_right,
              std::string const& rec_sep,
              std::string const& rec_left,
              std::string const& rec_right,
              std::string const& esc)
    : d_{d},
      begin_{begin},
      end_{end},
      set_sep_{set_sep},
      set_left_{set_left},
      set_right_{set_right},
      vec_sep_{vec_sep},
      vec_left_{vec_left},
      vec_right_{vec_right},
      rec_sep_{rec_sep},
      rec_left_{rec_left},
      rec_right_{rec_right},
      esc_{esc}
  {
  }

  template <
    typename T,
    typename = std::enable_if_t<type::is_basic<T>::value>
  >
  trial<void> operator()(T const&) const
  {
    return data_parse<type::to_data<T>>();
  }

  trial<void> operator()(none const&) const
  {
    return error{"cannot parse a none type"};
  }

  trial<void> operator()(type::enumeration const&) const
  {
    return error{"cannot parse an enum type"};
  }

  trial<void> operator()(type::vector const& t) const
  {
    return data_parse<vector>(t.elem(), vec_sep_, vec_left_, vec_right_, esc_);
  }

  trial<void> operator()(type::set const& t) const
  {
    return data_parse<set>(t.elem(), set_sep_, set_left_, set_right_, esc_);
  }

  trial<void> operator()(type::table const&) const
  {
    return error{"cannot parse tables (yet)"};
  }

  trial<void> operator()(type::record const& t) const
  {
    return data_parse<record>(t, rec_sep_, rec_left_, rec_right_, esc_);
  }

  trial<void> operator()(type::alias const& a) const
  {
    return visit(*this, a.type());
  }

  template <typename T, typename... Args>
  trial<void> data_parse(Args&&... args) const
  {
    T x;
    auto t = parse(x, begin_, end_, std::forward<Args>(args)...);
    if (! t)
      return t.error();

    d_ = data{std::move(x)};

    return nothing;
  }

private:
  data& d_;
  Iterator& begin_;
  Iterator end_;
  std::string const& set_sep_;
  std::string const& set_left_;
  std::string const& set_right_;
  std::string const& vec_sep_;
  std::string const& vec_left_;
  std::string const& vec_right_;
  std::string const& rec_sep_;
  std::string const& rec_left_;
  std::string const& rec_right_;
  std::string const& esc_;
};

} // namespace detail
} // namespace vast

// These require a complete definition of the class data.
#include "vast/detail/parser/data.h"
#include "vast/detail/parser/skipper.h"

namespace vast {
namespace detail {

template <typename Iterator>
trial<void> parse(data& d, Iterator& begin, Iterator end)
{
  detail::parser::data<Iterator> grammar;
  detail::parser::skipper<Iterator> skipper;

  if (phrase_parse(begin, end, grammar, skipper, d) && begin == end)
    return nothing;
  else
    return error{"failed to parse data"};
}

} // namespace detail
} // namespace vast

namespace vast {

template <typename Iterator>
trial<void> parse(data& d, Iterator& begin, Iterator end,
                  type const& t = {},
                  std::string const& set_sep = ", ",
                  std::string const& set_left = "{",
                  std::string const& set_right = "}",
                  std::string const& vec_sep = ", ",
                  std::string const& vec_left = "[",
                  std::string const& vec_right = "]",
                  std::string const& rec_sep = ", ",
                  std::string const& rec_left = "(",
                  std::string const& rec_right = ")",
                  std::string const& esc = "\\")
{
  if (is<none>(t))
    return detail::parse(d, begin, end);

  detail::data_parser<Iterator> p{d, begin, end,
                                  set_sep, set_left, set_right,
                                  vec_sep, vec_left, vec_right,
                                  rec_sep, rec_left, rec_right,
                                  esc};
  return visit(p, t);
}

//
// Conversion
//

trial<void> convert(vector const& v, util::json& j);
trial<void> convert(set const& v, util::json& j);
trial<void> convert(table const& v, util::json& j);
trial<void> convert(record const& v, util::json& j);
trial<void> convert(data const& v, util::json& j);

} // namespace vast

#endif
