#ifndef VAST_UTIL_JSON
#define VAST_UTIL_JSON

#include <map>
#include <string>
#include <vector>
#include "vast/util/none.h"
#include "vast/util/print.h"
#include "vast/util/variant.h"

namespace vast {
namespace util {

/// A JSON data type.
class json
{
public:
  struct array;
  struct object;

  /// A JSON number value.
  using number = long double;

  /// A JSON value.
  using value = variant<
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

  enum class type : value::tag_type
  {
    null = 0,
    boolean = 1,
    number = 2,
    string = 3,
    array = 4,
    object = 5
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

  /// Retrieves the type of the JSON value.
  /// @returns The type of this value.
  type which() const
  {
    return static_cast<type>(value_.which());
  }

private:
  value value_;

private:
  template <typename Iterator>
  struct printer
  {
    printer(Iterator& out)
      : out{out}
    {}

    trial<void> operator()(none const&)
    {
      return print("null", out);
    }

    trial<void> operator()(bool b)
    {
      return print(b ? "true" : "false", out);
    }

    trial<void> operator()(json::number n)
    {
      auto str = std::to_string(n);

      json::number i;
      if (std::modf(n, &i) == 0.0)
        str.erase(str.find('.'), std::string::npos);
      else
        str.erase(str.find_last_not_of('0') + 1, std::string::npos);

      return print(str, out);
    }

    trial<void> operator()(std::string const& str)
    {
      auto t = print('"', out);
      if (! t)
        return t.error();

      t = print(str, out); // TODO: escape properly.
      if (! t)
        return t.error();

      return print('"', out);
    }

    trial<void> operator()(json::array const& a)
    {
      auto t = print('[', out);
      if (! t)
        return t.error();

      t = print_delimited(", ", a.begin(), a.end(), out);
      if (! t)
        return t.error();

      return print(']', out);
    }

    trial<void> operator()(json::object const& o)
    {
      auto t = print('{', out);
      if (! t)
        return t.error();

      auto begin = o.begin();
      auto end = o.end();
      while (begin != end)
      {
        t = (*this)(begin->first);
        if (! t)
          return t.error();

        t = print(": ", out);
        if (! t)
          return t.error();

        t = print(begin->second, out);
        if (! t)
          return t.error();

        ++begin;

        if (begin != end)
        {
          t = print(", ", out);
          if (! t)
            return t.error();
        }
      }

      return print('}', out);
    }

    Iterator& out;
  };

  template <typename Iterator>
  friend trial<void> print(type const& t, Iterator&& out)
  {
    switch (t)
    {
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
  friend trial<void> print(value const& v, Iterator&& out)
  {
    return apply_visitor(printer<Iterator>{out}, v);
  }

  template <typename Iterator>
  friend trial<void> print(json const& j, Iterator&& out)
  {
    return print(j.value_, out, tree, indent);
  }
};

} // namespace util
} // namespace vast

#endif
