#ifndef VAST_UTIL_JSON
#define VAST_UTIL_JSON

#include <map>
#include <string>
#include <vector>
#include "vast/util/none.h"
#include "vast/util/print.h"
#include "vast/util/operators.h"
#include "vast/util/string.h"
#include "vast/util/variant.h"

namespace vast {
namespace util {

/// A JSON data type.
class json : totally_ordered<json>
{
public:
  enum class type : uint8_t
  {
    null = 0,
    boolean = 1,
    number = 2,
    string = 3,
    array = 4,
    object = 5
  };

  struct array;
  struct object;

  /// A JSON number value.
  using number = long double;

  /// A JSON value.
  using value = basic_variant<
    type,
    none,
    bool,
    number,
    std::string,
    array,
    object
  >;

  /// Meta-function that converts a type into a JSON value.
  /// If conversion is impossible it returns `std::false_type`.
  template <typename T>
  using json_value = std::conditional_t<
    std::is_same<T, none>::value,
    none,
    std::conditional_t<
      std::is_same<T, bool>::value,
      bool,
      std::conditional_t<
        std::is_convertible<T, number>::value,
        number,
        std::conditional_t<
          std::is_convertible<T, std::string>::value,
          std::string,
          std::conditional_t<
            std::is_same<T, array>::value,
            array,
            std::conditional_t<
              std::is_same<T, object>::value,
              object,
              std::false_type
            >
          >
        >
      >
    >
  >;

  template <typename T>
  using jsonize = json_value<std::decay_t<T>>;

  /// A sequence of JSON values.
  struct array : std::vector<json>
  {
    using vector<json>::vector;
  };

  /// An associative data structure exposing key-value pairs with unique keys.
  struct object : std::map<std::string, json>
  {
    using map<std::string, json>::map;
  };

  /// Default-constructs a null JSON value.
  json() = default;

  /// Constructs a JSON value.
  /// @tparam The JSON type.
  /// @param x A JSON type.
  template <
    typename T,
    typename =
      // FIXME: make this SFINAE expression shorter.
      std::enable_if_t<! std::is_same<std::false_type, jsonize<T>>::value>
  >
  json(T&& x)
    : value_(jsonize<T>(std::forward<T>(x)))
  {
  }

private:
  value value_;

private:
  struct less_than
  {
    template <typename T>
    bool operator()(T const& x, T const& y)
    {
      return x < y;
    }

    template <typename T, typename U>
    bool operator()(T const&, U const&)
    {
      return false;
    }
  };

  struct equals
  {
    template <typename T>
    bool operator()(T const& x, T const& y)
    {
      return x == y;
    }

    template <typename T, typename U>
    bool operator()(T const&, U const&)
    {
      return false;
    }
  };

  friend bool operator<(json const& x, json const& y)
  {
    if (which(x) == which(y))
      return visit(less_than{}, x, y);
    else
      return which(x) < which(y);
  }

  friend bool operator==(json const& x, json const& y)
  {
    return visit(equals{}, x, y);
  }

  friend json::value& expose(json& j)
  {
    return j.value_;
  }

  friend json::value const& expose(json const& j)
  {
    return j.value_;
  }

  template <typename Iterator>
  struct printer
  {
    printer(Iterator& out, bool tree, size_t indent)
      : out_{out},
        tree_{tree},
        indent_{indent}
    {
    }

    trial<void> operator()(none const&)
    {
      return print("null", out_);
    }

    trial<void> operator()(bool b)
    {
      return print(b ? "true" : "false", out_);
    }

    trial<void> operator()(json::number n)
    {
      auto str = std::to_string(n);

      json::number i;
      if (std::modf(n, &i) == 0.0)
        str.erase(str.find('.'), std::string::npos);
      else
        str.erase(str.find_last_not_of('0') + 1, std::string::npos);

      return print(str, out_);
    }

    trial<void> operator()(std::string const& str)
    {
      auto t = print(json_escape(str), out_);
      if (! t)
        return t.error();

      return nothing;
    }

    trial<void> operator()(json::array const& a)
    {
      auto t = print('[', out_);
      if (! t)
        return t.error();

      if (! a.empty() && tree_)
      {
        ++depth_;
        *out_++ = '\n';
      }

      auto begin = a.begin();
      auto end = a.end();
      while (begin != end)
      {
        indent();

        t = visit(*this, *begin);
        if (! t)
          return t.error();

        ++begin;

        if (begin != end)
        {
          t = print(tree_ ? ",\n" : ", ", out_);
          if (! t)
            return t.error();
        }
      }

      if (! a.empty() && tree_)
      {
        --depth_;
        *out_++ = '\n';
        indent();
      }

      return print(']', out_);
    }

    trial<void> operator()(json::object const& o)
    {
      auto t = print('{', out_);
      if (! t)
        return t.error();

      if (! o.empty() && tree_)
      {
        ++depth_;
        *out_++ = '\n';
      }

      auto begin = o.begin();
      auto end = o.end();
      while (begin != end)
      {
        indent();

        t = (*this)(begin->first);
        if (! t)
          return t.error();

        t = print(": ", out_);
        if (! t)
          return t.error();

        t = visit(*this, begin->second);
        if (! t)
          return t.error();

        ++begin;

        if (begin != end)
        {
          t = print(tree_ ? ",\n" : ", ", out_);
          if (! t)
            return t.error();
        }
      }

      if (! o.empty() && tree_)
      {
        --depth_;
        *out_++ = '\n';
        indent();
      }

      return print('}', out_);
    }

    void indent()
    {
      if (! tree_)
        return;

      for (size_t i = 0; i < depth_ * indent_; ++i)
        *out_++ = ' ';
    }

    Iterator& out_;
    bool tree_;
    size_t indent_;
    size_t depth_ = 0;
  };

  template <typename Iterator>
  friend trial<void> print(type const& t, Iterator&& out)
  {
    switch (t)
    {
      default:
        return error{"missing switch case"};
      case type::null:
        return print("null", out);
      case type::boolean:
        return print("bool", out);
      case type::number:
        return print("number", out);
      case type::string:
        return print("string", out);
      case type::array:
        return print("array", out);
      case type::object:
        return print("object", out);
    }
  }

  template <typename Iterator>
  friend trial<void> print(json const& j, Iterator&& out,
                           bool tree = false, size_t indent = 2)
  {
    return visit(printer<Iterator>{out, tree, indent}, j);
  }
};

inline trial<void> convert(bool b, json& j)
{
  j = b;
  return nothing;
}

template <typename T>
auto convert(T x, json& j)
  -> std::enable_if_t<std::is_arithmetic<T>::value, trial<void>>
{
  j = json::number(x);
  return nothing;
}

inline trial<void> convert(char const* str, json& j)
{
  j = str;
  return nothing;
}

inline trial<void> convert(std::string const& str, json& j)
{
  j = str;
  return nothing;
}

template <typename T>
trial<void> convert(std::vector<T> const& v, json& j)
{
  json::array a;
  for (auto& x : v)
  {
    json i;
    auto t = convert(x, i);
    if (! t)
      return t.error();

    a.push_back(std::move(i));
  };

  j = std::move(a);

  return nothing;
}

template <typename K, typename V>
trial<void> convert(std::map<K, V> const& m, json& j)
{
  util::json::object o;
  for (auto& p : m)
  {
    json k;
    auto t = convert(p.first, k);
    if (! t)
      return t.error();

    json v;
    t = convert(p.second, v);
    if (! t)
      return t.error();

    o.emplace(to_string(k), std::move(v));
  };

  j = std::move(o);

  return nothing;
}

} // namespace util
} // namespace vast

#endif
