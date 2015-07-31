#ifndef VAST_CONCEPT_PRINTABLE_VAST_DATA_H
#define VAST_CONCEPT_PRINTABLE_VAST_DATA_H

#include "vast/data.h"
#include "vast/util/string.h"
#include "vast/concept/printable/numeric.h"
#include "vast/concept/printable/print.h"
#include "vast/concept/printable/string.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/vast/address.h"
#include "vast/concept/printable/vast/subnet.h"
#include "vast/concept/printable/vast/pattern.h"
#include "vast/concept/printable/vast/port.h"
#include "vast/concept/printable/vast/none.h"
#include "vast/concept/printable/vast/time.h"
#include "vast/concept/printable/vast/type.h"

namespace vast {

struct data_printer : printer<data_printer>
{
  using attribute = data;

  template <typename Iterator>
  struct visitor
  {
    visitor(Iterator& out)
      : out_{out}
    {
    }

    template <typename T>
    bool operator()(T const& x) const
    {
      return make_printer<T>{}.print(out_, x);
    }

    bool operator()(std::string const& str) const
    {
      return printers::any.print(out_, '"')
          && printers::str.print(out_, util::byte_escape(str, "\""))
          && printers::any.print(out_, '"');
    }

    Iterator& out_;
  };

  template <typename Iterator>
  bool print(Iterator& out, data const& d) const
  {
    return visit(visitor<Iterator>{out}, d);
  }
};

struct vector_printer : printer<vector_printer>
{
  using attribute = vector;

  template <typename Iterator>
  bool print(Iterator& out, vector const& v) const
  {
    using namespace printers;
    return any.print(out, '[')
        && detail::print_delimited(v.begin(), v.end(), out, ", ")
        && any.print(out, ']');
  }
};

struct set_printer : printer<set_printer>
{
  using attribute = set;

  template <typename Iterator>
  bool print(Iterator& out, set const& s) const
  {
    using namespace printers;
    return any.print(out, '{')
        && detail::print_delimited(s.begin(), s.end(), out, ", ")
        && any.print(out, '}');
  }
};

struct table_printer : printer<table_printer>
{
  using attribute = table;

  template <typename Iterator>
  bool print(Iterator& out, table const& t) const
  {
    using namespace printers;
    using vast::print;
    if (! any.print(out, '{'))
      return false;
    auto f = t.begin();
    auto l = t.end();
    while (f != l)
    {
      if (! (print(out, f->first)
             && str.print(out, " -> ")
             && print(out, f->second)))
        return false;
      if (++f != l && ! str.print(out, ", "))
        return false;
    }
    return any.print(out, '{');
  }
};

struct record_printer : printer<record_printer>
{
  using attribute = record;

  template <typename Iterator>
  bool print(Iterator& out, record const& r) const
  {
    using namespace printers;
    return any.print(out, '(')
        && detail::print_delimited(r.begin(), r.end(), out, ", ")
        && any.print(out, ')');
  }
};

template <>
struct printer_registry<vector>
{
  using type = vector_printer;
};

template <>
struct printer_registry<set>
{
  using type = set_printer;
};

template <>
struct printer_registry<table>
{
  using type = table_printer;
};

template <>
struct printer_registry<record>
{
  using type = record_printer;
};

template <>
struct printer_registry<data>
{
  using type = data_printer;
};

} // namespace vast

#endif
