#include "vast/data.h"

#include "vast/serialization/all.h"
#include "vast/util/json.h"
#include "vast/offset.h"

namespace vast {

data const* record::at(offset const& o) const
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

    r = get<record>(*v);
    if (! r)
      return nullptr;
  }

  return nullptr;
}

data::variant_type& expose(data& d)
{
  return d.data_;
}

data::variant_type const& expose(data const& d)
{
  return d.data_;
}

bool operator==(data const& lhs, data const& rhs)
{
  return lhs.data_ == rhs.data_;
}

bool operator!=(data const& lhs, data const& rhs)
{
  return lhs.data_ != rhs.data_;
}

bool operator<(data const& lhs, data const& rhs)
{
  if (which(lhs.data_) == data::tag::none
      || which(rhs.data_) == data::tag::none)
    return false;

  return lhs.data_ < rhs.data_;
}

bool operator<=(data const& lhs, data const& rhs)
{
  if (which(lhs.data_) == data::tag::none
      || which(rhs.data_) == data::tag::none)
    return false;

  return lhs.data_ <= rhs.data_;
}

bool operator>=(data const& lhs, data const& rhs)
{
  if (which(lhs.data_) == data::tag::none
      || which(rhs.data_) == data::tag::none)
    return false;

  return lhs.data_ >= rhs.data_;
}

bool operator>(data const& lhs, data const& rhs)
{
  if (which(lhs.data_) == data::tag::none
      || which(rhs.data_) == data::tag::none)
    return false;

  return lhs.data_ > rhs.data_;
}

void data::serialize(serializer& sink) const
{
  sink << data_;
}

void data::deserialize(deserializer& source)
{
  source >> data_;
}

namespace {

struct json_converter
{
  json_converter(util::json& j)
    : j_{j}
  {
  }

  trial<void> operator()(none) const
  {
    return nothing;
  }

  trial<void> operator()(std::string const& str) const
  {
    j_ = str;
    return nothing;
  }

  template <typename T>
  trial<void> operator()(T const& x) const
  {
    return convert(x, j_);
  }

  util::json& j_;
};

} // namespace <anonymous>

trial<void> convert(vector const& v, util::json& j)
{
  util::json::array values;
  for (auto& x : v)
  {
    util::json j;
    auto t = visit(json_converter{j}, x);
    if (! t)
      return t.error();

    values.push_back(std::move(j));
  };

  j = std::move(values);
  return nothing;
}

trial<void> convert(set const& s, util::json& j)
{
  util::json::array values;
  for (auto& x : s)
  {
    util::json j;
    auto t = visit(json_converter{j}, x);
    if (! t)
      return t.error();

    values.push_back(std::move(j));
  };

  j = std::move(values);
  return nothing;
}

trial<void> convert(table const& t, util::json& j)
{
  util::json::array values;
  for (auto& p : t)
  {
    util::json::array a;

    util::json j;
    auto r = visit(json_converter{j}, p.first);
    if (! r)
      return r.error();

    a.push_back(std::move(j));

    r = visit(json_converter{j}, p.second);
    if (! r)
      return r.error();

    a.push_back(std::move(j));
    values.emplace_back(std::move(a));
  };

  j = std::move(values);
  return nothing;
}

trial<void> convert(record const& r, util::json& j)
{
  util::json::array values;
  for (auto& x : r)
  {
    util::json j;
    auto t = visit(json_converter{j}, x);
    if (! t)
      return t.error();

    values.push_back(std::move(j));
  };

  j = std::move(values);
  return nothing;
}

trial<void> convert(data const& d, util::json& j)
{
  return visit(json_converter{j}, d);
}

} // namespace vast
