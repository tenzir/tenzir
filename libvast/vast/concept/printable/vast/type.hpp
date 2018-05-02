/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/vast/attribute.hpp"
#include "vast/type.hpp"

namespace vast {
namespace detail {

template <class T>
auto make_attr_printer(const T& x) {
  auto has_attrs = printers::eps.with([&] { return !x.attributes().empty(); });
  auto attrs = &has_attrs << ' ' << (attribute_printer{} % ' ');
  return ~attrs;
}

} // namespace detail

struct enumeration_type_printer : printer<enumeration_type_printer> {
  using attribute = enumeration_type;

  template <class Iterator>
  bool print(Iterator& out, const enumeration_type& e) const {
    using namespace printers;
    auto p = "enum {"_P << (str % ", ") << '}';
    auto a = detail::make_attr_printer(e);
    return (p << a)(out, e.fields, e.attributes());
  }
};

template <>
struct printer_registry<enumeration_type> {
  using type = enumeration_type_printer;
};

#define VAST_DEFINE_BASIC_TYPE_PRINTER(TYPE, DESC)                             \
  struct TYPE##_printer : printer<TYPE##_printer> {                            \
    using attribute = TYPE;                                                    \
                                                                               \
    template <class Iterator>                                               \
    bool print(Iterator& out, const TYPE& t) const {                           \
      using namespace printers;                                                \
      auto p = DESC##_P << detail::make_attr_printer(t);                       \
      return p.print(out, t.attributes());                                     \
    }                                                                          \
  };                                                                           \
                                                                               \
  template <>                                                                  \
  struct printer_registry<TYPE> {                                              \
    using type = TYPE##_printer;                                               \
  };

VAST_DEFINE_BASIC_TYPE_PRINTER(none_type, "none")
VAST_DEFINE_BASIC_TYPE_PRINTER(boolean_type, "bool")
VAST_DEFINE_BASIC_TYPE_PRINTER(integer_type, "int")
VAST_DEFINE_BASIC_TYPE_PRINTER(count_type, "count")
VAST_DEFINE_BASIC_TYPE_PRINTER(real_type, "real")
VAST_DEFINE_BASIC_TYPE_PRINTER(timespan_type, "duration")
VAST_DEFINE_BASIC_TYPE_PRINTER(timestamp_type, "time")
VAST_DEFINE_BASIC_TYPE_PRINTER(string_type, "string")
VAST_DEFINE_BASIC_TYPE_PRINTER(pattern_type, "pattern")
VAST_DEFINE_BASIC_TYPE_PRINTER(address_type, "addr")
VAST_DEFINE_BASIC_TYPE_PRINTER(subnet_type, "subnet")
VAST_DEFINE_BASIC_TYPE_PRINTER(port_type, "port")
#undef VAST_DEFINE_BASIC_TYPE_PRINTER

// For the implementation, see below. (Must come after type due to recursion.)
#define VAST_DECLARE_TYPE_PRINTER(TYPE)                                        \
  struct TYPE##_printer : printer<TYPE##_printer> {                            \
    using attribute = TYPE;                                                    \
                                                                               \
    template <class Iterator>                                               \
    bool print(Iterator& out, const TYPE&) const;                              \
  };                                                                           \
                                                                               \
  template <>                                                                  \
  struct printer_registry<TYPE> {                                              \
    using type = TYPE##_printer;                                               \
  };

VAST_DECLARE_TYPE_PRINTER(vector_type)
VAST_DECLARE_TYPE_PRINTER(set_type)
VAST_DECLARE_TYPE_PRINTER(map_type)
VAST_DECLARE_TYPE_PRINTER(record_type)
VAST_DECLARE_TYPE_PRINTER(alias_type)
#undef VAST_DECLARE_TYPE_PRINTER

namespace policy {

struct signature {};
struct name_only {};
struct type_only {};

} // namespace policy

template <class Policy>
struct type_printer : printer<type_printer<Policy>> {
  using attribute = type;

  constexpr static bool show_name
    = std::is_same<Policy, policy::signature>{}
      || std::is_same<Policy, policy::name_only>{};

  constexpr static bool show_type
    = std::is_same<Policy, policy::signature>{}
      || std::is_same<Policy, policy::type_only>{};

  static_assert(show_name || show_type, "must show something");

  template <class Iterator>
  bool print(Iterator& out, const type& t) const {
    if (show_name && !t.name().empty()) {
      auto guard = printers::eps.with([] { return show_type; });
      auto p = (printers::str << ~(&guard << " = "));
      if (!p(out, t.name()))
        return false;
    }
    if (show_type || t.name().empty()) {
      auto p = none_type_printer{}
             | boolean_type_printer{}
             | integer_type_printer{}
             | count_type_printer{}
             | real_type_printer{}
             | timespan_type_printer{}
             | timestamp_type_printer{}
             | string_type_printer{}
             | pattern_type_printer{}
             | address_type_printer{}
             | subnet_type_printer{}
             | port_type_printer{}
             | enumeration_type_printer{}
             | vector_type_printer{}
             | set_type_printer{}
             | map_type_printer{}
             | record_type_printer{}
             | alias_type_printer{}
             ;
      return p(out, expose(t));
    }
    return true;
  }
};

template <class Policy>
constexpr bool type_printer<Policy>::show_name;

template <class Policy>
constexpr bool type_printer<Policy>::show_type;

template <>
struct printer_registry<type> {
  using type = type_printer<policy::name_only>;
};

// -- implementation of recursive type printers ------------------------------

template <class Iterator>
bool vector_type_printer::print(Iterator& out, const vector_type& t) const {
  auto p = "vector<" << type_printer<policy::name_only>{} << '>';
  auto a = detail::make_attr_printer(t);
  return (p << a)(out, t.value_type, t.attributes());
}

template <class Iterator>
bool set_type_printer::print(Iterator& out, const set_type& t) const {
  auto p = "set<" << type_printer<policy::name_only>{} << '>';
  auto a = detail::make_attr_printer(t);
  return (p << a)(out, t.value_type, t.attributes());
}

template <class Iterator>
bool map_type_printer::print(Iterator& out, const map_type& t) const {
  using namespace printers;
  auto p =  "map<"
         << type_printer<policy::name_only>{}
         << ", "
         << type_printer<policy::name_only>{}
         << '>';
  auto a = detail::make_attr_printer(t);
  return (p << a)(out, t.key_type, t.value_type, t.attributes());
}

struct record_field_printer : printer<record_field_printer> {
  using attribute = record_field;

  template <class Iterator>
  bool print(Iterator& out, const record_field& f) const {
    auto p = printers::str << ": " << type_printer<policy::name_only>{};
    return p(out, f.name, f.type);
  }
};

template <>
struct printer_registry<record_field> {
  using type = record_field_printer;
};

template <class Iterator>
bool record_type_printer::print(Iterator& out, const record_type& t) const {
  auto p = "record{"_P << (record_field_printer{} % ", ") << '}';
  auto a = detail::make_attr_printer(t);
  return (p << a)(out, t.fields, t.attributes());
}

template <class Iterator>
bool alias_type_printer::print(Iterator& out, const alias_type& t) const {
  auto p = type_printer<policy::name_only>{};
  auto a = detail::make_attr_printer(t);
  return (p << a)(out, t.value_type, t.attributes());
}

namespace printers {

template <class Policy>
type_printer<Policy> type{};

} // namespace printers
} // namespace vast

