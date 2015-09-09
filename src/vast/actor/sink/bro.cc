#include "vast/time.h"
#include "vast/type.h"
#include "vast/actor/sink/bro.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/numeric/real.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"
#include "vast/concept/printable/vast/key.h"
#include "vast/util/assert.h"
#include "vast/util/fdoutbuf.h"
#include "vast/util/string.h"

using namespace std::string_literals;

namespace vast {
namespace sink {

constexpr char const* bro_state::format;

std::string bro_state::make_header(type const& t) {
  auto r = get<type::record>(t);
  VAST_ASSERT(r);
  std::string h;
  h += "#separator"s + ' ' + util::byte_escape(std::string{sep}) + '\n';
  h += "#set_separator"s + sep + set_separator + '\n';
  h += "#empty_field"s + sep + empty_field + '\n';
  h += "#unset_field"s + sep + unset_field + '\n';
  h += "#path"s + sep + t.name() + '\n';
  h += "#open"s + sep + to_string(time::now(), format) + '\n';
  h += "#fields";
  for (auto& e : type::record::each{*r})
    h += sep + to_string(e.key());
  h += "\n#types";
  for (auto& e : type::record::each{*r})
    h += sep + to_string(e.trace.back()->type);
  h += '\n';
  return h;
}

std::string bro_state::make_footer() {
  std::string f = "#close"s + sep + to_string(time::now(), format) + '\n';
  return f;
}

bro_state::bro_state(local_actor* self)
  : state{self, "bro-sink"} {
}

bro_state::~bro_state() {
  auto footer = make_footer();
  for (auto& pair : streams)
    if (pair.second)
      std::copy(footer.begin(), footer.end(),
                std::ostreambuf_iterator<char>(*pair.second));
}

namespace {

struct value_printer {
  using result_type = std::string;

  std::string operator()(none) const {
    return bro_state::unset_field;
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
    return str;
  }

  std::string operator()(port const& p) const {
    return std::to_string(p.number());
  }

  std::string operator()(record const& r) const {
    auto visitor = this;
    return util::join(r.begin(), r.end(), std::string{bro_state::sep},
                      [&](auto& d) { return visit(*visitor, d); });
  }

  template <typename C>
  auto operator()(C const& c) const
    -> std::enable_if_t<
         std::is_same<C, vector>::value || std::is_same<C, set>::value,
         std::string
        > {
    if (c.empty())
      return bro_state::empty_field;
    auto visitor = this;
    return util::join(c.begin(), c.end(), bro_state::set_separator,
                      [&](auto& x) { return visit(*visitor, x); });
  }

  std::string operator()(table const&) const {
    return bro_state::unset_field; // Not yet supported by Bro.
  }
};

} // namespace <anonymous>

bool bro_state::process(event const& e) {
  auto& t = e.type();
  if (!is<type::record>(t)) {
    VAST_ERROR_AT(self, "cannot process non-record events");
    return false;
  }
  std::ostream* os = nullptr;
  if (dir.empty()) {
    if (streams.empty()) {
      VAST_DEBUG_AT(self, "creates a new stream for STDOUT");
      auto sb = std::make_unique<util::fdoutbuf>(1);
      auto out = std::make_unique<std::ostream>(sb.release());
      auto i = streams.emplace("", std::move(out));
      auto header = make_header(t);
      std::copy(header.begin(), header.end(),
                std::ostreambuf_iterator<char>(*i.first->second));
    }
    os = streams.begin()->second.get();
  } else {
    auto i = streams.find(t.name());
    if (i != streams.end()) {
      os = i->second.get();
      VAST_ASSERT(os != nullptr);
    } else {
      VAST_DEBUG_AT(self, "creates new stream for event", t.name());
      if (!exists(dir)) {
        auto d = mkdir(dir);
        if (!d) {
          VAST_ERROR_AT(self, "failed to create directory:", d.error());
          self->quit(exit::error);
          return false;
        }
      } else if (!dir.is_directory()) {
        VAST_ERROR_AT(self, "got existing non-directory path:", dir);
        self->quit(exit::error);
        return false;
      }
      auto filename = dir / (t.name() + ".log");
      auto fos = std::make_unique<std::ofstream>(filename.str());
      auto header = make_header(t);
      std::copy(header.begin(), header.end(),
                std::ostreambuf_iterator<char>(*fos));
      auto i = streams.emplace("", std::move(fos));
      os = i.first->second.get();
    }
  }
  VAST_ASSERT(os != nullptr);
  // FIXME: print to stream directly and don't go through std::string.
  auto str = visit(value_printer{}, e);
  str += '\n';
  std::copy(str.begin(), str.end(), std::ostreambuf_iterator<char>(*os));
  return os->good();
}

behavior bro(stateful_actor<bro_state>* self, path p) {
  if (p != "-")
    self->state.dir = std::move(p);
  return make(self);
}

} // namespace sink
} // namespace vast
