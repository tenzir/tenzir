#include "vast/time.h"
#include "vast/type.h"

#include "vast/sink/bro.h"
#include "vast/util/string.h"

namespace vast {
namespace sink {

// FIXME: why does the linker complain when not declaring this variable?
constexpr char const* bro::format;

std::string bro::make_header(type const& t)
{
  std::string h;
  h += std::string{"#separator "} + util::byte_escape(std::string{sep}) + '\n';
  h += std::string{"#set_separator"} + sep + set_separator + '\n';
  h += std::string{"#empty_field"} + sep + empty_field + '\n';
  h += std::string{"#unset_field"} + sep + unset_field + '\n';
  h += std::string{"#path"} + sep + to_string(t.name()) + '\n';
  h += std::string{"#open"} + sep + to_string(now(), format) + '\n';

  h += "#fields";
  t.each([&](key const& k, offset const&) { h += sep + to_string(k); });
  h += '\n';

  h += "#types";
  t.each(
      [&](key const&, offset const& o)
      {
        assert(t.at(o));
        h += sep + to_string(*t.at(o), false);
      });
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

  template <typename T>
  std::string operator()(T const& x) const
  {
    return to_string(x);
  }

  std::string operator()(value_invalid) const
  {
    return bro::unset_field;
  }

  std::string operator()(type_tag) const
  {
    return bro::unset_field;
  }

  std::string operator()(int64_t i) const
  {
    return std::to_string(i);
  }

  std::string operator()(uint64_t u) const
  {
    return std::to_string(u);
  }

  std::string operator()(double d) const
  {
    return to_string(d, 6);
  }

  std::string operator()(time_range tr) const
  {
    double d;
    convert(tr, d);
    return (*this)(d);
  }

  std::string operator()(time_point tp) const
  {
    return (*this)(tp.since_epoch());
  }

  std::string operator()(string const& str) const
  {
    return std::string{str.begin(), str.end()};
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
      str += value::visit(*begin++, *this);

      if (begin != end)
        str += bro::sep;
      else
        break;
    }

    return str;
  }

  std::string operator()(vector const& v) const
  {
    if (v.empty())
      return bro::empty_field;

    std::string str;
    auto begin = v.begin();
    auto end = v.end();

    while (begin != end)
    {
      str += value::visit(*begin++, *this);

      if (begin != end)
        str += bro::set_separator;
      else
        break;
    }

    return str;
  }

  std::string operator()(set const& s) const
  {
    return (*this)(static_cast<vector const&>(s));
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
  auto t = e.type();

  stream* s = nullptr;
  if (dir_.empty())
  {
    if (streams_.empty())
    {
      VAST_LOG_ACTOR_DEBUG("creates a new stream for STDOUT");
      auto i = streams_.emplace("",  std::make_unique<stream>("-"));
      auto header = make_header(*t);
      if (! i.first->second->write(header.begin(), header.end()))
        return false;
    }

    s = streams_.begin()->second.get();
  }
  else
  {
    auto& strm = streams_[to_string(t->name())];
    s = strm.get();
    if (! s)
    {
      VAST_LOG_ACTOR_DEBUG("creates new stream for event " << t->name());

      if (! exists(dir_))
      {
        auto t = mkdir(dir_);
        if (! t)
        {
          VAST_LOG_ACTOR_ERROR("failed to create directory: " << t.error());
          quit(exit::error);
          return false;
        }
      }
      else if (! dir_.is_directory())
      {
        VAST_LOG_ACTOR_ERROR("got existing non-directory path: " << dir_);
        quit(exit::error);
        return false;
      }

      strm = std::make_unique<stream>(dir_ / (t->name() + ".log"));
      s = strm.get();

      auto header = make_header(*t);
      if (! s->write(header.begin(), header.end()))
        return false;
    }
  }

  assert(s != nullptr);

  auto str = value_printer{}(static_cast<record const&>(e));
  str += '\n';

  return s->write(str.begin(), str.end());
}

std::string bro::describe() const
{
  return "bro-sink";
}

} // namespace sink
} // namespace vast
