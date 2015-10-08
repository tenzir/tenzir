#ifndef VAST_DATA_H
#define VAST_DATA_H

#include <iterator>
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
#include "vast/offset.h"
#include "vast/optional.h"
#include "vast/time.h"
#include "vast/type.h"
#include "vast/trial.h"
#include "vast/util/flat_set.h"
#include "vast/util/meta.h"
#include "vast/util/operators.h"
#include "vast/util/string.h"

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

class record : public std::vector<data> {
  using super = std::vector<vast::data>;

public:
  /// Enables recursive record iteration.
  class each : public util::range_facade<each> {
  public:
    struct range_state {
      vast::data const& data() const;
      util::stack::vector<8, vast::data const*> trace;
      vast::offset offset;
    };

    each(record const& r);

  private:
    friend util::range_facade<each>;

    range_state const& state() const {
      return state_;
    }

    bool next();

    range_state state_;
    util::stack::vector<8, record const*> records_;
  };

  using super::vector;

  record() = default;

  explicit record(super v) : super{std::move(v)} { }

  using super::at;

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
                || std::is_same<T, time::point>::value
                || std::is_same<T, time::duration>::value
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
        || std::is_same<T, time::point>::value
        || std::is_same<T, time::duration>::value
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
    time_duration,
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
    time::point,
    time::duration,
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
  data(optional<T>&& o)
    : data{o ? std::move(*o) : data{nil}} {
  }

  friend bool operator==(data const& lhs, data const& rhs);
  friend bool operator<(data const& lhs, data const& rhs);

  friend variant_type& expose(data& d);
  friend variant_type const& expose(data const& d);

private:
  variant_type data_;
};

} // namespace vast

#endif
