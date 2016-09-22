#ifndef VAST_CONCEPT_PRINTABLE_VAST_TYPE_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_TYPE_HPP

#include "vast/type.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/vast/none.hpp"

namespace vast {

namespace detail {

template <typename Iterator>
bool print_attributes(Iterator& out, type const& t) {
  return t.attributes.empty()
         || ((printers::any.print(out, ' ')
              && detail::print_delimited(t.attributes.begin(),
                                         t.attributes.end(), out, ' ')));
}

} // namespace detail

struct attribute_printer : printer<attribute_printer> {
  using type = attribute;

  template <typename Iterator>
  bool print(Iterator& out, attribute const& attr) const {
    using namespace printers;
    if (!any.print(out, '&'))
      return false;
    if (!str.print(out, attr.key))
      return false;
    return !attr.value || str.print(out, *attr.value);
    }
  }
};

struct type_enumeration_printer : printer<type_enumeration_printer> {
  using type = enumeration_type;

  template <typename Iterator>
  bool print(Iterator& out, enumeration_type const& e) {
    using vast::print;
    auto f = e.fields().begin();
    auto l = e.fields().end();
    return print(out, "enum {") && detail::print_delimited(f, l, out, ", ")
           && print(out, '}') && detail::print_attributes(out, e);
  }
};

#define VAST_DEFINE_BASIC_TYPE_PRINTER(id, desc)                               \
  struct basic_type_printer##id : printer<basic_type_printer##id> {            \
    using attribute = id_type;                                                 \
                                                                               \
    template <typename Iterator>                                               \
    bool print(Iterator& out, id_type const& n) const {                        \
      using namespace printers;                                                \
      using vast::print;                                                       \
      return str.print(out, desc) && detail::print_attributes(out, n);         \
    }                                                                          \
  };                                                                           \
                                                                               \
  template <>                                                                  \
  struct printer_registry<id_type> {                                           \
    using type = basic_type_printer##id;                                       \
  };

VAST_DEFINE_BASIC_TYPE_PRINTER(boolean, "bool")
VAST_DEFINE_BASIC_TYPE_PRINTER(integer, "int")
VAST_DEFINE_BASIC_TYPE_PRINTER(count, "count")
VAST_DEFINE_BASIC_TYPE_PRINTER(real, "real")
VAST_DEFINE_BASIC_TYPE_PRINTER(time_point, "time")
VAST_DEFINE_BASIC_TYPE_PRINTER(time_interval, "interval")
VAST_DEFINE_BASIC_TYPE_PRINTER(time_duration, "duration")
VAST_DEFINE_BASIC_TYPE_PRINTER(time_period, "period")
VAST_DEFINE_BASIC_TYPE_PRINTER(string, "string")
VAST_DEFINE_BASIC_TYPE_PRINTER(pattern, "pattern")
VAST_DEFINE_BASIC_TYPE_PRINTER(address, "addr")
VAST_DEFINE_BASIC_TYPE_PRINTER(subnet, "subnet")
VAST_DEFINE_BASIC_TYPE_PRINTER(port, "port")
#undef VAST_DEFINE_BASIC_TYPE_PRINTER

struct type_vector_printer : printer<type_vector_printer> {
  using attribute = vector_type;

  template <typename Iterator>
  bool print(Iterator& out, vector_type const& v) const {
    using vast::print;
    return print(out, "vector<") && print(out, v.elem()) && print(out, '>')
           && detail::print_attributes(out, v);
  }
};

struct type_set_printer : printer<type_set_printer> {
  using attribute = set_type;

  template <typename Iterator>
  bool print(Iterator& out, set_type const& s) const {
    using vast::print;
    return print(out, "set<") && print(out, s.elem()) && print(out, '>')
           && detail::print_attributes(out, s);
  }
};

struct type_table_printer : printer<type_table_printer> {
  using attribute = table_type;

  template <typename Iterator>
  bool print(Iterator& out, table_type const& t) const {
    using vast::print;
    return print(out, "table<") && print(out, t.key()) && print(out, ", ")
           && print(out, t.value()) && print(out, '>')
           && detail::print_attributes(out, t);
  }
};

struct record_field_printer : printer<record_field_printer> {
  using attribute = record_field;

  template <typename Iterator>
  bool print(Iterator& out, record_field const& f) const {
    using vast::print;
    return print(out, f.name) && print(out, ": ") && print(out, f.type);
  }
};

struct type_record_printer : printer<type_record_printer> {
  using attribute = record_type;

  template <typename Iterator>
  bool print(Iterator& out, record_type const& r) const {
    using vast::print;
    auto f = r.fields.begin();
    auto l = r.fields.end();
    return print(out, "record {") && detail::print_delimited(f, l, out, ", ")
           && print(out, '}') && detail::print_attributes(out, r);
  }
};

struct type_alias_printer : printer<type_alias_printer> {
  using attribute = alias_type;

  template <typename Iterator>
  bool print(Iterator& out, alias_type const& a) const {
    using vast::print;
    return print(out, a.type()) && detail::print_attributes(out, a);
  }
};

namespace policy {

struct signature {};
struct name_only {};
struct type_only {};

} // namespace policy

template <typename Policy>
struct type_printer : printer<type_printer<Policy>> {
  using attribute = type;

  constexpr static bool show_name
    = std::is_same<Policy, policy::signature>{}
      || std::is_same<Policy, policy::name_only>{};

  constexpr static bool show_type
    = std::is_same<Policy, policy::signature>{}
      || std::is_same<Policy, policy::type_only>{};

  template <typename Iterator>
  struct visitor {
    visitor(Iterator& out) : out_{out} { }

    bool operator()(none) const {
      return printers::str.print(out_, "none");
    }

    template <typename T>
    bool operator()(T const& x) const {
      using vast::print;
      return print(out_, x);
    }

    Iterator& out_;
  };

  template <typename Iterator>
  bool print(Iterator& out, type const& t) const {
    using namespace printers;
    using vast::print;
    auto imbued = show_name && !t.name().empty();
    if (imbued) {
      if (!str.print(out, t.name()))
        return false;
      if (show_type && !str.print(out, " = "))
        return false;
    }
    if (!imbued || show_type) {
      return visit(visitor<Iterator>{out}, t);
    }
    return true;
  }
};

template <typename Policy>
constexpr bool type_printer<Policy>::show_name;

template <typename Policy>
constexpr bool type_printer<Policy>::show_type;

template <>
struct printer_registry<attribute> {
  using type = attribute_printer;
};

template <>
struct printer_registry<enumeration_type> {
  using type = type_enumeration_printer;
};

template <>
struct printer_registry<vector_type> {
  using type = type_vector_printer;
};

template <>
struct printer_registry<set_type> {
  using type = type_set_printer;
};

template <>
struct printer_registry<table_type> {
  using type = type_table_printer;
};

template <>
struct printer_registry<record_field> {
  using type = record_field_printer;
};

template <>
struct printer_registry<record_type> {
  using type = type_record_printer;
};

template <>
struct printer_registry<alias_type> {
  using type = type_alias_printer;
};

template <>
struct printer_registry<tag_type> {
  using type = type_tag_printer;
};

template <>
struct printer_registry<type> {
  using type = type_printer<policy::name_only>;
};

namespace printers {

template <typename Policy>
type_printer<Policy> type_tag{};

template <typename Policy>
type_printer<Policy> type{};

} // namespace printers
} // namespace vast

#endif
