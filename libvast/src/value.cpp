/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/json.hpp"
#include "vast/value.hpp"

namespace vast {

bool value::type(const vast::type& t) {
  if (!type_check(t, data_))
    return false;
  type_ = t;
  return true;
}

const type& value::type() const {
  return type_;
}

const vast::data& value::data() const {
  return data_;
}

value flatten(const value& v) {
  return {flatten(v.data()), flatten(v.type())};
}

bool operator==(const value& lhs, const value& rhs) {
  return lhs.data_ == rhs.data_;
}

bool operator!=(const value& lhs, const value& rhs) {
  return lhs.data_ != rhs.data_;
}

bool operator<(const value& lhs, const value& rhs) {
  return lhs.data_ < rhs.data_;
}

bool operator<=(const value& lhs, const value& rhs) {
  return lhs.data_ <= rhs.data_;
}

bool operator>=(const value& lhs, const value& rhs) {
  return lhs.data_ >= rhs.data_;
}

bool operator>(const value& lhs, const value& rhs) {
  return lhs.data_ > rhs.data_;
}

detail::data_variant& expose(value& v) {
  return expose(v.data_);
}

bool convert(const value& v, json& j) {
  json::object o;
  if (!convert(v.type(), o["type"]))
    return false;
  if (!convert(v.data(), o["data"], v.type()))
    return false;
  j = std::move(o);
  return true;
}
} // namespace vast
