//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/base.hpp"

#include <algorithm>

namespace vast {

base base::uniform(value_type b, size_t n) {
  return base{vector_type(n, b)};
}

base::base(vector_type xs) : values_{std::move(xs)} {
}

base::base(std::initializer_list<value_type> xs) : values_{std::move(xs)} {
}

bool base::well_defined() const {
  return !values_.empty() && std::all_of(begin(), end(), [](auto x) {
    return x >= 2;
  });
}

bool base::empty() const {
  return values_.empty();
}

size_t base::size() const {
  return values_.size();
}

size_t base::memusage() const {
  return values_.capacity() * sizeof(value_type);
}

typename base::value_type& base::operator[](size_t i) {
  return values_[i];
}

typename base::value_type base::operator[](size_t i) const {
  return values_[i];
}

const typename base::value_type* base::data() const {
  return values_.data();
}

typename base::iterator base::begin() {
  return values_.begin();
}

typename base::const_iterator base::begin() const {
  return values_.begin();
}

typename base::iterator base::end() {
  return values_.end();
}

typename base::const_iterator base::end() const {
  return values_.end();
}

bool operator==(const base& x, const base& y) {
  return x.values_ == y.values_;
}

} // namespace vast
