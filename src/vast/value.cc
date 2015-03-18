#include "vast/value.h"

#include "vast/util/json.h"
#include "vast/offset.h"

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
  o["type"] = to_string(v.type());

  auto t = convert(v.data(), o["data"]);
  if (! t)
    return t.error();

  j = std::move(o);
  return nothing;
}

} // namespace vast
