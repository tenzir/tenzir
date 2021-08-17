//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/vast/attribute.hpp"
#include "vast/legacy_type.hpp"

namespace vast {
namespace detail {

template <class T>
auto make_attr_printer(const T& x) {
  auto has_attrs = printers::eps.with([&] { return !x.attributes().empty(); });
  auto attrs = &has_attrs << ' ' << (attribute_printer{} % ' ');
  return ~attrs;
}

} // namespace detail

struct legacy_enumeration_type_printer
  : printer_base<legacy_enumeration_type_printer> {
  using attribute = legacy_enumeration_type;

  template <class Iterator>
  bool print(Iterator& out, const legacy_enumeration_type& e) const {
    using namespace printers;
    using namespace printer_literals;
    auto p = "enum {"_P << (str % ", ") << '}';
    auto a = detail::make_attr_printer(e);
    return (p << a)(out, e.fields, e.attributes());
  }
};

template <>
struct printer_registry<legacy_enumeration_type> {
  using type = legacy_enumeration_type_printer;
};

#define VAST_DEFINE_BASIC_TYPE_PRINTER(TYPE, DESC)                             \
  struct TYPE##_printer : printer_base<TYPE##_printer> {                       \
    using attribute = TYPE;                                                    \
                                                                               \
    template <class Iterator>                                                  \
    bool print(Iterator& out, const TYPE& t) const {                           \
      using namespace printers;                                                \
      using namespace printer_literals;                                        \
      auto p = DESC##_P << detail::make_attr_printer(t);                       \
      return p.print(out, t.attributes());                                     \
    }                                                                          \
  };                                                                           \
                                                                               \
  template <>                                                                  \
  struct printer_registry<TYPE> {                                              \
    using type = TYPE##_printer;                                               \
  };

VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_none_type, "none")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_bool_type, "bool")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_integer_type, "int")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_count_type, "count")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_real_type, "real")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_duration_type, "duration")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_time_type, "time")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_string_type, "string")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_pattern_type, "pattern")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_address_type, "addr")
VAST_DEFINE_BASIC_TYPE_PRINTER(legacy_subnet_type, "subnet")
#undef VAST_DEFINE_BASIC_TYPE_PRINTER

// For the implementation, see below. (Must come after type due to recursion.)
#define VAST_DECLARE_TYPE_PRINTER(TYPE)                                        \
  struct TYPE##_printer : printer_base<TYPE##_printer> {                       \
    using attribute = TYPE;                                                    \
                                                                               \
    template <class Iterator>                                                  \
    bool print(Iterator& out, const TYPE&) const;                              \
  };                                                                           \
                                                                               \
  template <>                                                                  \
  struct printer_registry<TYPE> {                                              \
    using type = TYPE##_printer;                                               \
  };

VAST_DECLARE_TYPE_PRINTER(legacy_list_type)
VAST_DECLARE_TYPE_PRINTER(legacy_map_type)
VAST_DECLARE_TYPE_PRINTER(legacy_record_type)
VAST_DECLARE_TYPE_PRINTER(legacy_alias_type)
#undef VAST_DECLARE_TYPE_PRINTER

namespace policy {

struct signature {};
struct name_only {};
struct type_only {};

} // namespace policy

template <class Policy>
struct type_printer : printer_base<type_printer<Policy>> {
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
      // clang-format off
      auto p = legacy_none_type_printer{}
             | legacy_bool_type_printer{}
             | legacy_integer_type_printer{}
             | legacy_count_type_printer{}
             | legacy_real_type_printer{}
             | legacy_duration_type_printer{}
             | legacy_time_type_printer{}
             | legacy_string_type_printer{}
             | legacy_pattern_type_printer{}
             | legacy_address_type_printer{}
             | legacy_subnet_type_printer{}
             | legacy_enumeration_type_printer{}
             | legacy_list_type_printer{}
             | legacy_map_type_printer{}
             | legacy_record_type_printer{}
             | legacy_alias_type_printer{}
             ;
      // clang-format on
      return p(out, t);
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
bool legacy_list_type_printer::print(Iterator& out,
                                     const legacy_list_type& t) const {
  auto p = "list<" << type_printer<policy::name_only>{} << '>';
  auto a = detail::make_attr_printer(t);
  return (p << a)(out, t.value_type, t.attributes());
}

template <class Iterator>
bool legacy_map_type_printer::print(Iterator& out,
                                    const legacy_map_type& t) const {
  using namespace printers;
  auto p =  "map<"
         << type_printer<policy::name_only>{}
         << ", "
         << type_printer<policy::name_only>{}
         << '>';
  auto a = detail::make_attr_printer(t);
  return (p << a)(out, t.key_type, t.value_type, t.attributes());
}

struct record_field_printer : printer_base<record_field_printer> {
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
bool legacy_record_type_printer::print(Iterator& out,
                                       const legacy_record_type& t) const {
  using namespace printer_literals;
  auto p = "record{"_P << (record_field_printer{} % ", ") << '}';
  auto a = detail::make_attr_printer(t);
  return (p << a)(out, t.fields, t.attributes());
}

template <class Iterator>
bool legacy_alias_type_printer::print(Iterator& out,
                                      const legacy_alias_type& t) const {
  auto p = type_printer<policy::name_only>{};
  auto a = detail::make_attr_printer(t);
  return (p << a)(out, t.value_type, t.attributes());
}

namespace printers {

template <class Policy>
type_printer<Policy> type{};

} // namespace printers
} // namespace vast

