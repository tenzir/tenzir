#include "vast/time.h"
#include "vast/type.h"
#include "vast/actor/sink/bro.h"
#include "vast/io/algorithm.h"
#include "vast/util/assert.h"
#include "vast/util/string.h"

using namespace std::string_literals;

namespace vast {
namespace sink {

constexpr char const* bro::format;

std::string bro::make_header(type const& t)
{
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
    h += sep + to_string(e.trace.back()->type, 0);
  h += '\n';
  return h;
}

std::string bro::make_footer()
{
  std::string f = "#close"s + sep + to_string(time::now(), format) + '\n';
  return f;
}

bro::bro(path p)
  : base<bro>{"bro-sink"}
{
  // An empty directory means we write to standard output.
  if (p != "-")
    dir_ = std::move(p);
  attach_functor( [=](uint32_t)
    {
      auto footer = make_footer();
      for (auto& p : streams_)
        if (p.second)
          io::copy(footer.begin(), footer.end(), *p.second);
      streams_.clear();
    });
}

namespace {

struct value_printer
{
  using result_type = std::string;

  std::string operator()(none) const
  {
    return bro::unset_field;
  }

  template <typename T>
  std::string operator()(T&& x) const
  {
    return to_string(x);
  }

  std::string operator()(integer i) const
  {
    return std::to_string(i);
  }

  std::string operator()(count c) const
  {
    return std::to_string(c);
  }

  std::string operator()(real r) const
  {
    return to_string(r, 6);
  }

  std::string operator()(time::point point) const
  {
    return (*this)(point.time_since_epoch());
  }

  std::string operator()(time::duration dur) const
  {
    double d;
    convert(dur, d);
    return (*this)(d);
  }

  std::string operator()(std::string const& str) const
  {
    return str;
  }

  std::string operator()(port const& p) const
  {
    return std::to_string(p.number());
  }

  std::string operator()(record const& r) const
  {
    auto visitor = this;
    return util::join(r.begin(), r.end(), std::string{bro::sep},
                      [&](auto& d) { return visit(*visitor, d); });
  }

  template <typename C>
  auto operator()(C const& c) const
    -> std::enable_if_t<
         std::is_same<C, vector>::value || std::is_same<C, set>::value,
         std::string
       >
  {
    if (c.empty())
      return bro::empty_field;
    auto visitor = this;
    return util::join(c.begin(), c.end(), bro::set_separator,
                      [&](auto& x) { return visit(*visitor, x); });
  }

  std::string operator()(table const&) const
  {
    // Not yet supported by Bro.
    return bro::unset_field;
  }
};

} // namespace <anonymous>

bool bro::process(event const& e)
{
  auto& t = e.type();
  if (! is<type::record>(t))
  {
    VAST_ERROR(this, "cannot process non-record events");
    return false;
  }
  io::file_output_stream* os = nullptr;
  if (dir_.empty())
  {
    if (streams_.empty())
    {
      VAST_DEBUG(this, "creates a new stream for STDOUT");
      auto fos = std::make_unique<io::file_output_stream>("-");
      auto i = streams_.emplace("",  std::move(fos));
      auto header = make_header(t);
      if (! io::copy(header.begin(), header.end(), *i.first->second))
        return false;
    }
    os = streams_.begin()->second.get();
  }
  else
  {
    auto i = streams_.find(t.name());
    if (i != streams_.end())
    {
      os = i->second.get();
      VAST_ASSERT(os != nullptr);
    }
    else
    {
      VAST_DEBUG(this, "creates new stream for event", t.name());
      if (! exists(dir_))
      {
        auto d = mkdir(dir_);
        if (! d)
        {
          VAST_ERROR(this, "failed to create directory:", d.error());
          quit(exit::error);
          return false;
        }
      }
      else if (! dir_.is_directory())
      {
        VAST_ERROR(this, "got existing non-directory path:", dir_);
        quit(exit::error);
        return false;
      }
      auto filename = dir_ / (t.name() + ".log");
      auto fos = std::make_unique<io::file_output_stream>(filename);
      auto header = make_header(t);
      if (! (io::copy(header.begin(), header.end(), *fos) && fos->flush()))
        return false;
      auto i = streams_.emplace("",  std::move(fos));
      os = i.first->second.get();
    }
  }
  VAST_ASSERT(os != nullptr);
  auto str = visit(value_printer{}, e);
  str += '\n';
  return io::copy(str.begin(), str.end(), *os) && os->flush();
}

} // namespace sink
} // namespace vast
