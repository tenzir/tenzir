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

#include "vast/view.hpp"

#include "vast/detail/overload.hpp"

namespace vast {

// -- pattern_view ------------------------------------------------------------

pattern_view::pattern_view(const pattern& x) : pattern_{x.string()} {
  // nop
}

std::string_view pattern_view::string() const {
  return pattern_;
}

bool operator==(pattern_view x, pattern_view y) noexcept {
  return x.string() == y.string();
}

bool operator<(pattern_view x, pattern_view y) noexcept {
  return x.string() < y.string();
}

// -- data_view ---------------------------------------------------------------

namespace {

template <template <class> class Predicate, class T>
bool compare(T x, T y) {
  auto cmp = [](auto xs, auto ys) {
    return xs && ys && Predicate{}(*xs, *ys);
  };
  return caf::visit(detail::overload(
    [=](const auto& x, const auto& y) {
      using lhs_type = std::decay_t<decltype(x)>;
      using rhs_type = std::decay_t<decltype(y)>;
      if constexpr (std::is_same_v<lhs_type, rhs_type>) {
        return Predicate{}(x, y);
      } else {
        return false;
      }
    },
    [=](view<vector> xs, view<vector> ys) {
      return cmp(xs, ys);
    },
    [=](view<set> xs, view<set> ys) {
      return cmp(xs, ys);
    },
    [=](view<map> xs, view<map> ys) {
      return cmp(xs, ys);
    }
  ), x, y);
}

} // namespace <anonymous>

bool operator==(const data_view& x, const data_view& y) {
  return x.index() == y.index() && compare<std::equal_to>(x, y);
}

bool operator!=(const data_view& x, const data_view& y) {
  return !(x == y);
}

bool operator<(const data_view& x, const data_view& y) {
  if (y.valueless_by_exception())
    return false;
  if (x.valueless_by_exception())
    return true;
  auto i = x.index();
  auto j = y.index();
  return i != j ? i < j : compare<std::less>(x, y);
}

bool operator>(const data_view& x, const data_view& y) {
  if (x.valueless_by_exception())
    return false;
  if (y.valueless_by_exception())
    return true;
  auto i = x.index();
  auto j = y.index();
  return i != j ? i < j : compare<std::greater>(x, y);
}

bool operator<=(const data_view& x, const data_view& y) {
  return !(x > y);
}

bool operator>=(const data_view& x, const data_view& y) {
  return !(x < y);
}

// -- default_vector_view -----------------------------------------------------

default_vector_view::default_vector_view(const vector& xs) : xs_{xs} {
  // nop
}

default_vector_view::value_type default_vector_view::at(size_type i) const {
  return make_data_view(xs_[i]);
}

default_vector_view::size_type default_vector_view::size() const noexcept {
  return xs_.size();
}

// -- default_set_view --------------------------------------------------------

default_set_view::default_set_view(const set& xs) : xs_{xs} {
  // nop
}

default_set_view::value_type default_set_view::at(size_type i) const {
  return make_data_view(*std::next(xs_.begin(), i));
}

default_set_view::size_type default_set_view::size() const noexcept {
  return xs_.size();
}

// -- default_map_view --------------------------------------------------------

default_map_view::default_map_view(const map& xs) : xs_{xs} {
  // nop
}

default_map_view::value_type default_map_view::at(size_type i) const {
  auto& [key, value] = *std::next(xs_.begin(), i);
  return {make_data_view(key), make_data_view(value)};
}

default_map_view::size_type default_map_view::size() const noexcept {
  return xs_.size();
}

// -- make_view ---------------------------------------------------------------

data_view make_view(const data& x) {
  return caf::visit([](const auto& z) { return make_data_view(z); }, x);
}

// -- materialization ----------------------------------------------------------

std::string materialize(std::string_view x) {
  return std::string{x};
}

pattern materialize(pattern_view x) {
  return pattern{std::string{x.string()}};
}

namespace {

auto materialize(std::pair<data_view, data_view> x) {
  return std::pair(materialize(x.first), materialize(x.second));
}

template <class Result, class T>
Result materialize_container(const T& xs) {
  Result result;
  if (xs)
    for (auto x : *xs)
      result.insert(result.end(), materialize(x));
  return result;
}

} // namespace <anonymous>

vector materialize(vector_view_ptr xs) {
  return materialize_container<vector>(xs);
}

set materialize(set_view_ptr xs) {
  return materialize_container<set>(xs);
}

map materialize(map_view_ptr xs) {
  return materialize_container<map>(xs);
}

data materialize(data_view x) {
  return caf::visit([](auto y) { return data{materialize(y)}; }, x);
}

} // namespace vast
