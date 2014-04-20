#include "vast/schema.h"

#include <fstream>
#include "vast/serialization.h"
#include "vast/io/container_stream.h"
#include "vast/util/convert.h"

namespace vast {

trial<schema> schema::merge(schema const& s1, schema const& s2)
{
  schema merged = s1;

  for (auto& t2 : s2.types_)
  {
    auto t1 = s1.find_type(t2->name());
    if (t1 && *t1 != *t2)
      return error{"type clash: " + to_string(*t1) + " <--> " + to_string(*t2)};

    merged.types_.push_back(t2);
  }

  for (auto& e2 : s2.events_)
  {
    auto e1 = s1.find_event(e2.name);
    if (e1 && *e1 != e2)
      return error{"event clash: " + to_string(*e1) + " <--> " + to_string(e2)};

    merged.events_.push_back(e2);
  }

  return std::move(merged);
}

bool schema::add(type_ptr t)
{
  // Do not allow types with duplicate names.
  if (! t->name().empty() && find_type(t->name()))
    return false;

  // Do not allow an unnamed type twice.
  if (! find_type_info(t->info()).empty() && t->name().empty())
    return false;;

  types_.push_back(std::move(t));

  return true;
}

bool schema::add(event_info ei)
{
  if (find_event(ei.name))
    return false;

  events_.push_back(std::move(ei));

  return true;
}

size_t schema::types() const
{
  return types_.size();
}

size_t schema::events() const
{
  return events_.size();
}

type_ptr schema::find_type(string const& name) const
{
  for (auto& t : types_)
    if (t->name() == name)
      return t;

  return {};
}

type_ptr schema::find_type(string const& name, offset const& off) const
{
  if (off.empty())
    return {};

  auto ei = find_event(name);
  return ei ? ei->at(off) : type_ptr{};
}

std::vector<type_ptr> schema::find_type_info(type_info const& ti) const
{
  std::vector<type_ptr> types;
  for (auto& t : types_)
    if (t->info() == ti)
      types.push_back(t);

  return types;
}

std::multimap<string, offset>
schema::find_offsets(std::vector<string> const& ids) const
{
  if (ids.empty())
    return std::multimap<string, offset>{};

  std::multimap<string, offset> result;
  for (auto& ei : events_)
    for (auto& o : ei.find_suffix(ids))
      result.emplace(ei.name, std::move(o));

  return result;
}

event_info const* schema::find_event(string const& name) const
{
  for (auto& ei : events_)
    if (ei.name == name)
      return &ei;

  return nullptr;
}

void schema::serialize(serializer& sink) const
{
  sink << to_string(*this);
}

void schema::deserialize(deserializer& source)
{
  std::string str;
  source >> str;
  auto begin = str.begin();
  auto end = str.end();
  extract(begin, end, *this);
}

bool operator==(schema const& x, schema const& y)
{
  if (x.types_.size() != y.types_.size())
    return false;

  for (size_t i = 0; i < x.types_.size(); ++i)
    if (*x.types_[i] != *y.types_[i])
      return false;

  return x.events_ == y.events_;
}

} // namespace vast
