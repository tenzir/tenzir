#include <algorithm>

#include "vast/base.hpp"

namespace vast {

base base::uniform(value_type b, size_t n) {
  return base{vector_type(n, b)};
}

base::base(vector_type xs) : values_{std::move(xs)} {
}

base::base(std::initializer_list<value_type> xs) : values_{std::move(xs)} {
}

bool base::well_defined() const {
  return !values_.empty()
    && std::all_of(begin(), end(), [](auto x) { return x >= 2; });
}

bool base::empty() const {
  return values_.empty();
}

size_t base::size() const {
  return values_.size();
}

typename base::value_type& base::operator[](size_t i) {
  return values_[i];
}

typename base::value_type base::operator[](size_t i) const {
  return values_[i];
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

} // namespace vast
