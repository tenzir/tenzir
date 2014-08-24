#include "vast/schema.h"

#include <fstream>
#include "vast/serialization.h"
#include "vast/io/container_stream.h"

namespace vast {

trial<schema> schema::merge(schema const& s1, schema const& s2)
{
  schema merged = s1;

  for (auto& t2 : s2.types_)
  {
    auto t1 = s1.find_type(t2.name());
    if (! t1)
      merged.types_.push_back(t2);
    else if (*t1 != t2)
      return error{"type clash: ", *t1, " <--> ", t2};
  }

  return std::move(merged);
}

trial<void> schema::add(type t)
{
  if (is<none>(t))
    return error{"instance of invalid_type"};

  if (t.name().empty() && find_types(t).empty())
    return error{"duplicate unnamed typed: ", t};

  if (! t.name().empty())
    if (auto existing = find_type(t.name()))
    {
      if (*existing == t)
        return nothing;
      else
        return error{"clash in types with same name (existing <--> added): ",
                     to_string(*existing, false), " <--> ", to_string(t, false)};
    }

  types_.push_back(std::move(t));

  return nothing;
}

type const* schema::find_type(std::string const& name) const
{
  for (auto& t : types_)
    if (t.name() == name)
      return &t;

  return {};
}

std::vector<type> schema::find_types(type const& t) const
{
  std::vector<type> types;
  for (auto& ty : types_)
    if (ty == t)
      types.push_back(t);

  return types;
}

schema::const_iterator schema::begin() const
{
  return types_.begin();
}

schema::const_iterator schema::end() const
{
  return types_.end();
}

size_t schema::size() const
{
  return types_.size();
}

bool schema::empty() const
{
  return types_.empty();
}

void schema::serialize(serializer& sink) const
{
  sink << to_string(*this);
}

void schema::deserialize(deserializer& source)
{
  std::string str;
  source >> str;
  auto lval = str.begin();
  auto s = parse<schema>(lval, str.end());
  if (s)
    *this = *s;
}

bool operator==(schema const& x, schema const& y)
{
  return x.types_ == y.types_;
}

} // namespace vast
