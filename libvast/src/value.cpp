#include "vast/json.hpp"
#include "vast/value.hpp"

namespace vast {

bool value::type(vast::type const& t) {
  if (!type_check(t, data_))
    return false;
  type_ = t;
  return true;
}

type const& value::type() const {
  return type_;
}

vast::data const& value::data() const {
  return data_;
}

value flatten(value const& v) {
  return {flatten(v.data()), flatten(v.type())};
}

bool operator==(value const& lhs, value const& rhs) {
  return lhs.data_ == rhs.data_;
}

bool operator!=(value const& lhs, value const& rhs) {
  return lhs.data_ != rhs.data_;
}

bool operator<(value const& lhs, value const& rhs) {
  return lhs.data_ < rhs.data_;
}

bool operator<=(value const& lhs, value const& rhs) {
  return lhs.data_ <= rhs.data_;
}

bool operator>=(value const& lhs, value const& rhs) {
  return lhs.data_ >= rhs.data_;
}

bool operator>(value const& lhs, value const& rhs) {
  return lhs.data_ > rhs.data_;
}

detail::data_variant& expose(value& v) {
  return expose(v.data_);
}

bool convert(value const& v, json& j) {
  json::object o;
  if (!convert(v.type(), o["type"]))
    return false;
  if (!convert(v.data(), o["data"], v.type()))
    return false;
  j = std::move(o);
  return true;
}
} // namespace vast
