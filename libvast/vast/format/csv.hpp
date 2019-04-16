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

#include <caf/none.hpp>

#include "vast/config.hpp"

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/detail/string.hpp"
#include "vast/format/printer_writer.hpp"

namespace vast::format::csv {

struct value_printer : printer<value_printer> {
  using attribute = event;

  // TODO: agree on reasonable values.
  static constexpr char separator[] = ",";
  static constexpr char set_separator[] = " | ";
  static constexpr char empty[] = "\"\"";

  template <class Iterator>
  struct renderer {
    renderer(Iterator out) : out_{out} {
    }

    template <class T>
    bool operator()(const T&, caf::none_t) {
      return true;
    }

    template <class T, class U>
    bool operator()(const T&, const U& x) {
      return make_printer<U>{}.print(out_, x);
    }

    bool operator()(const real_type&, real r) {
      return real_printer<real, 6>{}.print(out_, r);
    }

    bool operator()(const string_type&, const std::string& str) {
      static auto escaper = detail::make_double_escaper("\"|");
      auto p = '"' << printers::escape(escaper) << '"';
      return p.print(out_, str);
    }

    bool operator()(const record_type& r, const vector& v) {
      VAST_ASSERT(!v.empty());
      VAST_ASSERT(r.fields.size() == v.size());
      using printers::eps;
      size_t i = 0;
      auto elem = eps.with([&] {
        auto result = caf::visit(*this, r.fields[i].type, v[i]);
        ++i;
        return result;
      });
      auto p = elem % separator;
      return p.print(out_, v);
    }

    bool operator()(const vector_type& t, const vector& v) {
      return render(v, t.value_type, set_separator);
    }

    bool operator()(const set_type& t, const set& s) {
      return render(s, t.value_type, set_separator);
    }

    bool operator()(const map_type&, const map&) {
      return false; // not yet supported
    }

  private:
    template <class Container, class Sep>
    bool render(Container& c, const type& t, const Sep& sep) {
      using printers::eps;
      if (c.empty())
        return printers::str.print(out_, empty);
      auto guard = [&](const data& x) { return caf::visit(*this, t, x); };
      auto elem = eps.with(guard);
      auto p = (elem % sep);
      return p.print(out_, c);
    }

    Iterator out_;
  };

  template <class Iterator>
  bool print(Iterator& out, const event& e) const {
    using namespace printers;
    // Print a new header each time we encounter a new event type.
    auto header_guard = [&] {
      if (e.type() == event_type)
        return true;
      event_type = e.type();
      auto hdr = "type,id,timestamp"s;
      if (auto r = caf::get_if<record_type>(&e.type()))
        for (auto& i : record_type::each{*r})
          hdr += ',' + i.key();
      else
        hdr += ",data";
      auto p = printers::str << '\n';
      return p.print(out, hdr);
    };
    auto header = eps.with(header_guard);
    // Print event data.
    auto name_guard = [](const std::string& x) { return !x.empty(); };
    auto name = printers::str.with(name_guard);
    auto f = [&] { caf::visit(renderer<Iterator>{out}, e.type(), e.data()); };
    auto ev = eps ->* f;
    auto p = header << name << ',' << ev;
    return p(out, e.type().name());
  }

  // FIXME: relax print() constness constraint?!
  mutable type event_type;
};

class writer : public printer_writer<value_printer>{
public:
  using printer_writer<value_printer>::printer_writer;

  const char* name() const {
    return "csv-writer";
  }
};

} // namespace vast::format::csv

