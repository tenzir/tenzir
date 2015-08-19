#ifndef VAST_CONCEPT_PRINTABLE_VAST_TYPE_H
#define VAST_CONCEPT_PRINTABLE_VAST_TYPE_H

#include "vast/type.h"
#include "vast/concept/printable/print.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/detail/print_delimited.h"
#include "vast/concept/printable/string/any.h"
#include "vast/concept/printable/string/string.h"
#include "vast/concept/printable/vast/none.h"

namespace vast {
namespace detail {

template <typename Iterator>
bool print_attributes(Iterator& out, type const& t) {
  return t.attributes().empty()
         || ((printers::any.print(out, ' ')
              && detail::print_delimited(t.attributes().begin(),
                                         t.attributes().end(), out, ' ')));
}

} // namespace detail

struct type_attribute_printer : printer<type_attribute_printer> {
  using type = type::attribute;

  template <typename Iterator>
  bool print(Iterator& out, type::attribute const& attr) const {
    using namespace printers;
    if (!any.print(out, '&'))
      return false;
    switch (attr.key) {
      default:
        return str.print(out, "invalid");
      case type::attribute::skip:
        return str.print(out, "skip");
      case type::attribute::default_:
        return str.print(out, "default=\"") && str.print(out, attr.value)
               && any.print(out, '"');
    }
  }
};

struct type_enumeration_printer : printer<type_enumeration_printer> {
  using type = type::enumeration;

  template <typename Iterator>
  bool print(Iterator& out, type::enumeration const& e) {
    using vast::print;
    auto f = e.fields().begin();
    auto l = e.fields().end();
    return print(out, "enum {") && detail::print_delimited(f, l, out, ", ")
           && print(out, '}') && detail::print_attributes(out, e);
  }
};

#define VAST_DEFINE_BASIC_TYPE_PRINTER(id, desc)                               \
  struct basic_type_printer##id : printer<basic_type_printer##id> {            \
    using attribute = type::id;                                                \
                                                                               \
    template <typename Iterator>                                               \
    bool print(Iterator& out, type::id const& n) const {                       \
      using namespace printers;                                                \
      using vast::print;                                                       \
      return str.print(out, desc) && detail::print_attributes(out, n);         \
    }                                                                          \
  };                                                                           \
                                                                               \
  template <>                                                                  \
  struct printer_registry<type::id> {                                          \
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
  using attribute = type::vector;

  template <typename Iterator>
  bool print(Iterator& out, type::vector const& v) const {
    using vast::print;
    return print(out, "vector<") && print(out, v.elem()) && print(out, '>')
           && detail::print_attributes(out, v);
  }
};

struct type_set_printer : printer<type_set_printer> {
  using attribute = type::set;

  template <typename Iterator>
  bool print(Iterator& out, type::set const& s) const {
    using vast::print;
    return print(out, "set<") && print(out, s.elem()) && print(out, '>')
           && detail::print_attributes(out, s);
  }
};

struct type_table_printer : printer<type_table_printer> {
  using attribute = type::table;

  template <typename Iterator>
  bool print(Iterator& out, type::table const& t) const {
    using vast::print;
    return print(out, "table<") && print(out, t.key()) && print(out, ", ")
           && print(out, t.value()) && print(out, '>')
           && detail::print_attributes(out, t);
  }
};

struct type_record_field_printer : printer<type_record_field_printer> {
  using attribute = type::record::field;

  template <typename Iterator>
  bool print(Iterator& out, type::record::field const& f) const {
    using vast::print;
    return print(out, f.name) && print(out, ": ") && print(out, f.type);
  }
};

struct type_record_printer : printer<type_record_printer> {
  using attribute = type::record;

  template <typename Iterator>
  bool print(Iterator& out, type::record const& r) const {
    using vast::print;
    auto f = r.fields().begin();
    auto l = r.fields().end();
    return print(out, "record {") && detail::print_delimited(f, l, out, ", ")
           && print(out, '}') && detail::print_attributes(out, r);
  }
};

struct type_alias_printer : printer<type_alias_printer> {
  using attribute = type::alias;

  template <typename Iterator>
  bool print(Iterator& out, type::alias const& a) const {
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
    visitor(Iterator& out) : out_{out} {
    }

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
struct printer_registry<type::attribute> {
  using type = type_attribute_printer;
};

template <>
struct printer_registry<type::enumeration> {
  using type = type_enumeration_printer;
};

template <>
struct printer_registry<type::vector> {
  using type = type_vector_printer;
};

template <>
struct printer_registry<type::set> {
  using type = type_set_printer;
};

template <>
struct printer_registry<type::table> {
  using type = type_table_printer;
};

template <>
struct printer_registry<type::record::field> {
  using type = type_record_field_printer;
};

template <>
struct printer_registry<type::record> {
  using type = type_record_printer;
};

template <>
struct printer_registry<type::alias> {
  using type = type_alias_printer;
};

template <>
struct printer_registry<type> {
  using type = type_printer<policy::name_only>;
};

namespace printers {

template <typename Policy>
type_printer<Policy> type{};

} // namespace printers
} // namespace vast

#endif
