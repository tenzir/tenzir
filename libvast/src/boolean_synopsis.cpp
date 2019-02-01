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

#include "vast/boolean_synopsis.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include "vast/detail/assert.hpp"

namespace vast {

boolean_synopsis::boolean_synopsis(vast::type x) : synopsis{std::move(x)} {
  VAST_ASSERT(caf::holds_alternative<boolean_type>(type()));
}

void boolean_synopsis::add(data_view x) {
  VAST_ASSERT(caf::holds_alternative<view<boolean>>(x));
  auto b = caf::get<view<boolean>>(x);
  if (b)
    true_ = true;
  else
    false_ = true;
}

bool boolean_synopsis::lookup(relational_operator op, data_view rhs) const {
  if (auto b = caf::get_if<view<boolean>>(&rhs)) {
    if (op == equal)
      return *b ? true_ : false_;
    if (op == not_equal)
      return *b ? false_ : true_;
  }
  return false;
}

bool boolean_synopsis::equals(const synopsis& other) const noexcept {
  if (typeid(other) != typeid(boolean_synopsis))
    return false;
  auto& rhs = static_cast<const boolean_synopsis&>(other);
  return type() == rhs.type() && false_ == rhs.false_ && true_ == rhs.true_;
}

caf::error boolean_synopsis::serialize(caf::serializer& sink) const {
  return sink(false_, true_);
}

caf::error boolean_synopsis::deserialize(caf::deserializer& source) {
  return source(false_, true_);
}

} // namespace vast
