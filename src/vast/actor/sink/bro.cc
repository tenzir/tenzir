#include "vast/time.h"
#include "vast/type.h"

#include "vast/actor/sink/bro.h"
#include "vast/util/string.h"

namespace vast {
namespace sink {

constexpr char const* bro::format;

std::string bro::make_header(type const& t)
{
  auto r = get<type::record>(t);
  assert(r);

  std::string h;
  h += std::string{"#separator "} + util::byte_escape(std::string{sep}) + '\n';
  h += std::string{"#set_separator"} + sep + set_separator + '\n';
  h += std::string{"#empty_field"} + sep + empty_field + '\n';
  h += std::string{"#unset_field"} + sep + unset_field + '\n';
  h += std::string{"#path"} + sep + t.name() + '\n';
  h += std::string{"#open"} + sep + to_string(now(), format) + '\n';

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
  std::string f = std::string{"#close"} + sep + to_string(now(), format) + '\n';
  return f;
}

bro::bro(path p)
{
  if (p != "-")
    dir_ = std::move(p);

  attach_functor(
      [=](uint32_t)
      {
        auto footer = make_footer();
        for (auto& p : streams_)
          if (p.second)
            p.second->write(footer.begin(), footer.end());

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

  std::string operator()(time_point tp) const
  {
    return (*this)(tp.since_epoch());
  }

  std::string operator()(time_range tr) const
  {
    double d;
    convert(tr, d);
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
    std::string str;
    auto begin = r.begin();
    auto end = r.end();

    while (begin != end)
    {
      str += visit(*this, *begin++);

      if (begin != end)
        str += bro::sep;
      else
        break;
    }

    return str;
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

    std::string str;
    auto begin = c.begin();
    auto end = c.end();

    while (begin != end)
    {
      str += visit(*this, *begin++);

      if (begin != end)
        str += bro::set_separator;
      else
        break;
    }

    return str;
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

  stream* s = nullptr;
  if (dir_.empty())
  {
    if (streams_.empty())
    {
      VAST_DEBUG(this, "creates a new stream for STDOUT");
      auto i = streams_.emplace("",  std::make_unique<stream>("-"));
      auto header = make_header(t);
      if (! i.first->second->write(header.begin(), header.end()))
        return false;
    }

    s = streams_.begin()->second.get();
  }
  else
  {
    auto& strm = streams_[t.name()];
    s = strm.get();
    if (! s)
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

      strm = std::make_unique<stream>(dir_ / (t.name() + ".log"));
      s = strm.get();

      auto header = make_header(t);
      if (! s->write(header.begin(), header.end()))
        return false;
    }
  }

  assert(s != nullptr);

  auto str = visit(value_printer{}, e);
  str += '\n';

  return s->write(str.begin(), str.end());
}

std::string bro::name() const
{
  return "bro-sink";
}

} // namespace sink
} // namespace vast
