#ifndef VAST_FORMAT_CSV_HPP
#define VAST_FORMAT_CSV_HPP

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/detail/string.hpp"

#include "vast/format/writer.hpp"

namespace vast {
namespace format {
namespace csv {

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
    bool operator()(T const&, none) {
      return true;
    }

    template <class T, class U>
    auto operator()(T const&, U const& x)
    -> std::enable_if_t<!std::is_same<U, none>::value, bool> {
      return make_printer<U>{}.print(out_, x);
    }

    bool operator()(real_type const&, real r) {
      return real_printer<real, 6>{}.print(out_, r);
    }

    bool operator()(string_type const&, std::string const& str) {
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

    bool operator()(record_type const& r, vector const& v) {
      VAST_ASSERT(!v.empty());
      VAST_ASSERT(r.fields.size() == v.size());
      using printers::eps;
      size_t i = 0;
      auto f = this;
      auto elem = eps.with([&] {
        auto result = visit(*f, r.fields[i].type, v[i]);
        ++i;
        return result;
      });
      auto p = elem % separator;
      return p.print(out_, v);
    }

    bool operator()(vector_type const& t, vector const& v) {
      return render(v, t.value_type, set_separator);
    }

    bool operator()(set_type const& t, set const& s) {
      return render(s, t.value_type, set_separator);
    }

    bool operator()(table_type const&, table const&) {
      return false; // not yet supported
    }

  private:
    template <class Container, class Sep>
    bool render(Container& c, type const& t, Sep const& sep) {
      using printers::eps;
      using printers::str;
      if (c.empty())
        return str.print(out_, empty);
      auto f = this;
      auto elem = eps.with([&](data const& x) { return visit(*f, t, x); });
      auto p = (elem % sep);
      return p.print(out_, c);
    }

    Iterator out_;
  };

  template <class Iterator>
  bool print(Iterator& out, event const& e) const {
    using namespace printers;
    // Print a new header each time we encounter a new event type.
    auto header = eps.with([&] {
      if (e.type() == event_type)
        return true;
      event_type = e.type();
      auto hdr = "type,id,timestamp"s;
      auto r =  get_if<record_type>(e.type());
      if (!r)
        hdr += ",data";
      else
        for (auto& i : record_type::each{*r})
          hdr += ',' + to_string(i.key());
      auto p = str << chr<'\n'>;
      return p.print(out, hdr);
    });
    // Print event data.
    auto name = str.with([](std::string const& x) { return !x.empty(); });
    auto comma = chr<','>;
    auto ts = u64 ->* [](timestamp t) { return t.time_since_epoch().count(); };
    auto f = [&] { visit(renderer<Iterator>{out}, e.type(), e.data()); };
    auto ev = eps ->* f;
    auto p = header << name << comma << u64 << comma << ts << comma << ev;
    auto t = std::forward_as_tuple(e.type().name(), e.id(), e.timestamp());
    return p.print(out, t);
  }

  // FIXME: relax print() constness constraint?!
  mutable type event_type;
};

class writer : public format::writer<value_printer>{
public:
  using format::writer<value_printer>::writer;

  char const* name() const {
    return "csv-writer";
  }
};

} // namespace csv
} // namespace format
} // namespace vast

#endif
