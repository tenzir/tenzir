//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/bool_synopsis.hpp"

#include "tenzir/detail/assert.hpp"

namespace tenzir {

bool_synopsis::bool_synopsis(tenzir::type x) : synopsis{std::move(x)} {
  TENZIR_ASSERT(is<bool_type>(type()));
}

bool_synopsis::bool_synopsis(bool true_, bool false_)
  : synopsis{tenzir::type{bool_type{}}}, true_(true_), false_(false_) {
}

synopsis_ptr bool_synopsis::clone() const {
  return std::make_unique<bool_synopsis>(true_, false_);
}

void bool_synopsis::add(data_view x) {
  TENZIR_ASSERT(is<view<bool>>(x));
  if (as<view<bool>>(x)) {
    true_ = true;
  } else
    false_ = true;
}

size_t bool_synopsis::memusage() const {
  return sizeof(bool_synopsis);
}

std::optional<bool>
bool_synopsis::lookup(relational_operator op, data_view rhs) const {
  if (auto b = try_as<view<bool>>(&rhs)) {
    if (op == relational_operator::equal)
      return *b ? true_ : false_;
    if (op == relational_operator::not_equal)
      return *b ? false_ : true_;
  }
  return {};
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

bool bool_synopsis::inspect_impl(supported_inspectors& inspector) {
  return std::visit(
    [this](auto inspector) {
      return inspector.get().apply(false_) && inspector.get().apply(true_);
    },
    inspector);
}

} // namespace tenzir
