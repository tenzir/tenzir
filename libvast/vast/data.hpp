#ifndef VAST_DATA_HPP
#define VAST_DATA_HPP

#include <iterator>
#include <regex>
#include <string>
#include <vector>
#include <map>
#include <type_traits>

#include "vast/aliases.hpp"
#include "vast/address.hpp"
#include "vast/pattern.hpp"
#include "vast/subnet.hpp"
#include "vast/port.hpp"
#include "vast/none.hpp"
#include "vast/offset.hpp"
#include "vast/maybe.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"
#include "vast/util/flat_set.hpp"
#include "vast/util/meta.hpp"
#include "vast/util/operators.hpp"
#include "vast/util/string.hpp"

namespace vast {

class data;

class vector : public std::vector<data> {
  using super = std::vector<vast::data>;

public:
  using super::vector;

  vector() = default;

  explicit vector(super v) : super{std::move(v)} {
  }
};

class set : public util::flat_set<data> {
  using super = util::flat_set<vast::data>;

public:
  using super::flat_set;

  set() = default;

  explicit set(super s) : super(std::move(s)) {
  }

  explicit set(std::vector<vast::data>& v)
    : super(std::make_move_iterator(v.begin()),
            std::make_move_iterator(v.end())) {
  }

  explicit set(std::vector<vast::data> const& v) 
    : super(v.begin(), v.end()) {
  }
};

class table : public std::map<data, data> {
  using super = std::map<vast::data, vast::data>;

public:
  using super::map;
};

class data : util::totally_ordered<data> {
  friend access;

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
                || std::is_same<T, timestamp>::value
                || std::is_same<T, interval>::value
                || std::is_same<T, pattern>::value
                || std::is_same<T, address>::value
                || std::is_same<T, subnet>::value
                || std::is_same<T, port>::value
                || std::is_same<T, enumeration>::value
                || std::is_same<T, vector>::value
                || std::is_same<T, set>::value
                || std::is_same<T, table>::value,
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
        || std::is_same<T, timestamp>::value
        || std::is_same<T, interval>::value
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

  enum class tag : uint8_t {
    none,
    boolean,
    integer,
    count,
    real,
    timestamp,
    interval,
    string,
    pattern,
    address,
    subnet,
    port,
    enumeration,
    vector,
    set,
    table,
  };

  using variant_type = util::basic_variant<
    tag,
    none,
    boolean,
    integer,
    count,
    real,
    timestamp,
    interval,
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
    : data_(type<T>(std::forward<T>(x))) {
  }

  /// Constructs optional data.
  template <typename T>
  data(maybe<T>&& o)
    : data{o ? std::move(*o) : data{nil}} {
  }

  friend bool operator==(data const& lhs, data const& rhs);
  friend bool operator<(data const& lhs, data const& rhs);

  template <class Inspector>
  friend auto inspect(Inspector&f, data& d) {
    return f(d.data_);
  }

  friend variant_type& expose(data& d);
  friend variant_type const& expose(data const& d);

private:
  variant_type data_;
};

} // namespace vast

#endif
