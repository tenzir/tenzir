#include "vast/value.h"

#include "vast/offset.h"
#include "vast/util/assert.h"
#include "vast/util/json.h"

namespace vast {

bool value::type(vast::type const& t)
{
  if (! t.check(data_))
    return false;

  type_ = t;
  return true;
}

type const& value::type() const
{
  return type_;
}

data::variant_type& expose(value& v)
{
  return expose(v.data_);
}

data::variant_type const& expose(value const& v)
{
  return expose(v.data_);
}

bool operator==(value const& lhs, value const& rhs)
{
  return lhs.data_ == rhs.data_;
}

bool operator!=(value const& lhs, value const& rhs)
{
  return lhs.data_ != rhs.data_;
}

bool operator<(value const& lhs, value const& rhs)
{
  return lhs.data_ < rhs.data_;
}

bool operator<=(value const& lhs, value const& rhs)
{
  return lhs.data_ <= rhs.data_;
}

bool operator>=(value const& lhs, value const& rhs)
{
  return lhs.data_ >= rhs.data_;
}

bool operator>(value const& lhs, value const& rhs)
{
  return lhs.data_ > rhs.data_;
}

trial<void> convert(value const& v, util::json& j)
{
  util::json::object o;
  auto show_full_type = false;
  o["type"] = to_string(v.type(), show_full_type);
  if (! is<type::record>(v.type()))
  {
    o["data"] = util::to_json(v.data());
  }
  else
  {
    util::json::object rec;
    type::record::each record_range{*get<type::record>(v.type())};
    record::each data_range{*get<record>(v.data())};
    auto r = record_range.begin();
    auto d = data_range.begin();
    while (r != record_range.end())
    {
      rec[r->key().back()] = util::to_json(d->data());
      ++r;
      ++d;
    }
    VAST_ASSERT(d == data_range.end());
    o["data"] = std::move(rec);
  }
  j = std::move(o);
  return nothing;
}

} // namespace vast
