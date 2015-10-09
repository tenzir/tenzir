#include <iterator>
#include <ostream>

#include "vast/actor/sink/csv.h"
#include "vast/util/assert.h"

using namespace std::string_literals;

namespace vast {
namespace sink {
namespace {

struct csv_printer : printer<csv_printer> {
  using attribute = event;

  // TODO: agree on reasonable values.
  static constexpr auto set_sep = "+";
  static constexpr auto empty = "\"\"";

  struct visitor {
    std::string operator()(none) const {
      return {};
    }

    template <typename T>
    std::string operator()(T&& x) const {
      return to_string(x);
    }

    std::string operator()(integer i) const {
      return std::to_string(i);
    }

    std::string operator()(count c) const {
      return std::to_string(c);
    }

    std::string operator()(real r) const {
      std::string str;
      auto out = std::back_inserter(str);
      real_printer<real, 6>{}.print(out, r);
      return str;
    }

    std::string operator()(time::point point) const {
      return (*this)(point.time_since_epoch());
    }

    std::string operator()(time::duration dur) const {
      double d;
      convert(dur, d);
      return (*this)(d);
    }

    std::string operator()(std::string const& str) const {
      return '"' + util::replace_all(str, "\"", "\"\"") + '"';
    }

    std::string operator()(port const& p) const {
      return std::to_string(p.number());
    }

    std::string operator()(record const& r) const {
      auto visitor = this;
      return util::join(r.begin(), r.end(), ","s,
                        [&](auto& d) { return visit(*visitor, d); });
    }

    template <typename C>
    auto operator()(C const& c) const
      -> std::enable_if_t<
           std::is_same<C, vector>::value || std::is_same<C, set>::value,
           std::string
          > {
      if (c.empty())
        return empty;
      auto visitor = this;
      return util::join(c.begin(), c.end(), set_sep,
                        [&](auto& x) { return visit(*visitor, x); });
    }

    std::string operator()(table const&) const {
      return {}; // Not yet supported.
    }
  };

  template <typename Iterator>
  bool print(Iterator& out, event const& e) const {
    using namespace printers;
    if (e.type().name().empty() && !str.print(out, "<anonymous>"))
      return false;
    auto val = visit(visitor{}, e);
    return str.print(out, e.type().name())
        && any.print(out, ',')
        && u64.print(out, e.id())
        && any.print(out, ',')
        && u64.print(out, e.timestamp().time_since_epoch().count())
        && any.print(out, ',')
        && str.print(out, val);
  }
};

} // namespace <anonymous>

csv_state::csv_state(local_actor* self)
  : state{self, "csv-sink"} {
}

bool csv_state::process(event const& e) {
  VAST_ASSERT(! is<none>(e.type()));
  if (e.type() != type) {
    type = e.type();
    std::string header = "type,id,timestamp";
    auto r = get<type::record>(type);
    if (!r)
      header += ",data";
    else
      for (auto& i : type::record::each{*r})
        header += ',' + to_string(i.key());
    *out << header << '\n';
  }
  auto i = std::ostreambuf_iterator<char>{*out};
  return csv_printer{}.print(i, e) && print(i, '\n');
}

void csv_state::flush() {
  out->flush();
}

behavior csv(stateful_actor<csv_state>* self, std::ostream* out) {
  VAST_ASSERT(out != nullptr);
  self->state.out = std::unique_ptr<std::ostream>{out};
  return make(self);
}

} // namespace sink
} // namespace vast
