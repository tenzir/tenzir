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
    auto operator()(const T&, const U& x)
    -> std::enable_if_t<!std::is_same_v<U, caf::none_t>, bool> {
      return make_printer<U>{}.print(out_, x);
    }

    bool operator()(const real_type&, real r) {
      return real_printer<real, 6>{}.print(out_, r);
    }

    bool operator()(const string_type&, const std::string& str) {
      using printers::chr;
      using printers::eps;
      auto escape = [&] {
        auto f = str.begin();
        auto l = str.end();
        while (f != l)
          detail::double_escaper("\"|")(f, l, out_);
      };
      auto p = chr<'"'> << (eps ->* escape) << chr<'"'>;
      return p.print(out_, unused);
    }

    bool operator()(const record_type& r, const vector& v) {
      VAST_ASSERT(!v.empty());
      VAST_ASSERT(r.fields.size() == v.size());
      using printers::eps;
      size_t i = 0;
      auto f = this;
      auto elem = eps.with([&] {
        auto result = caf::visit(*f, r.fields[i].type, v[i]);
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
      using printers::str;
      if (c.empty())
        return str.print(out_, empty);
      auto f = this;
      auto elem = eps.with([&](const data& x) { return caf::visit(*f, t, x); });
      auto p = (elem % sep);
      return p.print(out_, c);
    }

    Iterator out_;
  };

  template <class Iterator>
  bool print(Iterator& out, const event& e) const {
    using namespace printers;
    // Print a new header each time we encounter a new event type.
    auto header = eps.with([&] {
      if (e.type() == event_type)
        return true;
      event_type = e.type();
      auto hdr = "type,id,timestamp"s;
      auto r =  caf::get_if<record_type>(&e.type());
      if (!r)
        hdr += ",data";
      else
        for (auto& i : record_type::each{*r})
          hdr += ',' + i.key();
      auto p = str << chr<'\n'>;
      return p.print(out, hdr);
    });
    // Print event data.
    auto name = str.with([](const std::string& x) { return !x.empty(); });
    auto comma = chr<','>;
    auto ts = u64 ->* [](timestamp t) { return t.time_since_epoch().count(); };
    auto f = [&] { caf::visit(renderer<Iterator>{out}, e.type(), e.data()); };
    auto ev = eps ->* f;
    auto p = header << name << comma << u64 << comma << ts << comma << ev;
    return p(out, e.type().name(), e.id(), e.timestamp());
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

