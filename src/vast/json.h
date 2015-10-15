#ifndef VAST_JSON
#define VAST_JSON

#include <map>
#include <string>
#include <vector>

#include "vast/none.h"
#include "vast/concept/printable/to.h"
#include "vast/util/operators.h"
#include "vast/util/variant.h"

namespace vast {

/// A JSON data type.
class json : util::totally_ordered<json> {
public:
  enum class type : uint8_t {
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
  using value = util::basic_variant<
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
  struct array : std::vector<json> {
    using super = std::vector<json>;
    using super::vector;

    array() = default;
    array(super const& s) : super(s) {
    }
    array(super&& s) : super(std::move(s)) {
    }
  };

  /// An associative data structure exposing key-value pairs with unique keys.
  struct object : std::map<std::string, json> {
    using super = std::map<std::string, json>;
    using super::map;

    object() = default;
    object(super const& s) : super(s) {
    }
    object(super&& s) : super(std::move(s)) {
    }
  };

  /// Default-constructs a null JSON value.
  json() = default;

  /// Constructs a JSON value.
  /// @tparam The JSON type.
  /// @param x A JSON type.
  template <
    typename T,
    typename =
      std::enable_if_t<! std::is_same<std::false_type, jsonize<T>>::value>
  >
  json(T&& x)
    : value_(jsonize<T>(std::forward<T>(x))) {
  }

private:
  value value_;

private:
  struct less_than {
    template <typename T>
    bool operator()(T const& x, T const& y) {
      return x < y;
    }

    template <typename T, typename U>
    bool operator()(T const&, U const&) {
      return false;
    }
  };

  struct equals {
    template <typename T>
    bool operator()(T const& x, T const& y) {
      return x == y;
    }

    template <typename T, typename U>
    bool operator()(T const&, U const&) {
      return false;
    }
  };

  friend bool operator<(json const& x, json const& y) {
    if (which(x) == which(y))
      return visit(less_than{}, x, y);
    else
      return which(x) < which(y);
  }

  friend bool operator==(json const& x, json const& y) {
    return visit(equals{}, x, y);
  }

  friend json::value& expose(json& j) {
    return j.value_;
  }

  friend json::value const& expose(json const& j) {
    return j.value_;
  }
};

inline bool convert(bool b, json& j) {
  j = b;
  return true;
}

template <typename T>
auto convert(T x, json& j)
  -> std::enable_if_t<std::is_arithmetic<T>::value, bool> {
  j = json::number(x);
  return true;
}

template <typename T>
auto convert(T&& x, json& j)
  -> std::enable_if_t<std::is_convertible<T, std::string>{}, bool> {
  j = std::string(std::forward<T>(x));
  return true;
}

template <typename T>
bool convert(std::vector<T> const& v, json& j) {
  json::array a;
  for (auto& x : v) {
    json i;
    if (!convert(x, i))
      return false;
    a.push_back(std::move(i));
  };
  j = std::move(a);
  return true;
}

template <typename K, typename V>
bool convert(std::map<K, V> const& m, json& j) {
  json::object o;
  for (auto& p : m) {
    auto k = to<std::string>(p.first);
    json v;
    if (!(k && convert(p.second, v)))
      return false;
    o.emplace(std::move(*k), std::move(v));
  };
  j = std::move(o);
  return true;
}

template <typename T, typename... Opts>
json to_json(T const& x, Opts&&... opts) {
  json j;
  if (convert(x, j, std::forward<Opts>(opts)...))
    return j;
  return {};
}

} // namespace vast

#endif
