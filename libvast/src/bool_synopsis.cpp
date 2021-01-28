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

#include "vast/bool_synopsis.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include "vast/detail/assert.hpp"

namespace vast {

bool_synopsis::bool_synopsis(vast::type x) : synopsis{std::move(x)} {
  VAST_ASSERT(caf::holds_alternative<bool_type>(type()));
}

bool_synopsis::bool_synopsis(bool true_, bool false_)
  : synopsis{bool_type{}}, true_(true_), false_(false_) {
}

void bool_synopsis::add(data_view x) {
  VAST_ASSERT(caf::holds_alternative<view<bool>>(x));
  if (caf::get<view<bool>>(x))
    true_ = true;
  else
    false_ = true;
}

size_t bool_synopsis::memusage() const {
  return sizeof(bool_synopsis);
}

caf::optional<bool> bool_synopsis::lookup(relational_operator op,
                                          data_view rhs) const {
  if (auto b = caf::get_if<view<bool>>(&rhs)) {
    if (op == equal)
      return *b ? true_ : false_;
    if (op == not_equal)
      return *b ? false_ : true_;
  }
  return caf::none;
}

bool bool_synopsis::equals(const synopsis& other) const noexcept {
  if (typeid(other) != typeid(bool_synopsis))
    return false;
  auto& rhs = static_cast<const bool_synopsis&>(other);
  return type() == rhs.type() && false_ == rhs.false_ && true_ == rhs.true_;
}

bool bool_synopsis::any_false() {
  return false_;
}

bool bool_synopsis::any_true() {
  return true_;
}

caf::error bool_synopsis::serialize(caf::serializer& sink) const {
  return sink(false_, true_);
}

caf::error bool_synopsis::deserialize(caf::deserializer& source) {
  return source(false_, true_);
}

} // namespace vast
