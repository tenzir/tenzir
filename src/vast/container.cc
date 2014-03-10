#include "vast/container.h"

#include <algorithm>
#include <cassert>
#include "vast/logger.h"
#include "vast/serialization.h"

namespace vast {

value const* record::at(offset const& o) const
{
  record const* r = this;
  for (size_t i = 0; i < o.size(); ++i)
  {
    auto& idx = o[i];
    if (idx >= r->size())
      return nullptr;
    auto v = &(*r)[idx];
    if (i + 1 == o.size())
      return v;
    if (! (v->which() == record_type && *v))
      return nullptr;
    r = &v->get<record>();
  }

  return nullptr;
}

value const* record::flat_at(size_t i) const
{
  size_t base = 0;
  return do_flat_at(i, base);
}

size_t record::flat_size() const
{
  size_t count = 0;
  each([&](value const&) { ++count; }, true);
  return count;
}

void record::each(std::function<void(value const&)> f, bool recurse) const
{
  for (auto& v : *this)
    if (recurse && v.which() == record_type && v)
      v.get<record>().each(f, recurse);
    else
      f(v);
}

bool record::any(std::function<bool(value const&)> f, bool recurse) const
{
  for (auto& v : *this)
  {
    if (recurse && v.which() == record_type)
    {
      if (v && v.get<record>().any(f, true))
        return true;
    }
    else if (f(v))
      return true;
  }
  return false;
}

bool record::all(std::function<bool(value const&)> f, bool recurse) const
{
  for (auto& v : *this)
  {
    if (recurse && v.which() == record_type)
    {
      if (v && ! v.get<record>().all(f, recurse))
        return false;
    }
    else if (! f(v))
      return false;
  }
  return true;
}

void
record::each_offset(std::function<void(value const&, offset const&)> f) const
{
  offset o;
  do_each_offset(f, o);
}

value const* record::do_flat_at(size_t i, size_t& base) const
{
  assert(base <= i);
  for (auto& v : *this)
  {
    if (v.which() == record_type)
    {
      auto result = v.get<record>().do_flat_at(i, base);
      if (result)
        return result;
    }
    else if (base++ == i)
      return &v;
  }

  return nullptr;
}

void record::do_each_offset(std::function<void(value const&, offset const&)> f,
                           offset& o) const
{
  o.push_back(0);
  for (auto& v : *this)
  {
    if (v && v.which() == record_type)
      v.get<record>().do_each_offset(f, o);
    else
      f(v, o);

    ++o.back();
  }

  o.pop_back();
}

void record::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << static_cast<super const&>(*this);
}

void record::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> static_cast<super&>(*this);
  VAST_LEAVE(VAST_THIS);
}

bool operator==(record const& x, record const& y)
{
  return static_cast<record::super const&>(x) ==
    static_cast<record::super const&>(y);
}

bool operator<(record const& x, record const& y)
{
  return static_cast<record::super const&>(x) <
    static_cast<record::super const&>(y);
}


void table::each(std::function<void(value const&, value const&)> f) const
{
  for (auto& p : *this)
    f(p.first, p.second);
}

bool table::any(std::function<bool(value const&, value const&)> f) const
{
  for (auto& p : *this)
    if (f(p.first, p.second))
      return true;
  return false;
}

bool table::all(std::function<bool(value const&, value const&)> f) const
{
  for (auto& p : *this)
    if (! f(p.first, p.second))
      return false;
  return true;
}

void table::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << static_cast<super const&>(*this);
}

void table::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> static_cast<super&>(*this);
  VAST_LEAVE(VAST_THIS);
}

bool operator==(table const& x, table const& y)
{
  return static_cast<table::super const&>(x) ==
    static_cast<table::super const&>(y);
}

bool operator<(table const& x, table const& y)
{
  return static_cast<table::super const&>(x) <
    static_cast<table::super const&>(y);
}

} // namespace vast
