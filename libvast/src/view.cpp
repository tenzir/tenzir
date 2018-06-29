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

bool operator==(pattern_view x, pattern_view y) noexcept {
  return x.pattern_ == y.pattern_;
}

std::string_view pattern_view::string() const {
  return pattern_;
}

// -- address_view ------------------------------------------------------------

address_view::address_view(const address& x) : data_{&x.data()} {
  // nop
}

const std::array<uint8_t, 16>& address_view::data() const {
  return *data_;
}

bool operator==(address_view x, address_view y) noexcept {
  return x.data_ == y.data_;
}

// -- subnet_view -------------------------------------------------------------

subnet_view::subnet_view(const subnet& x)
  : network_{x.network()},
    length_{x.length()} {
  // nop
}

address_view subnet_view::network() const {
  return network_;
}

uint8_t subnet_view::length() const {
  return length_;
}

bool operator==(subnet_view x, subnet_view y) noexcept {
  return x.network_ == y.network_ && x.length_ == y.length_;
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

// -- make_data ---------------------------------------------------------------

namespace {

auto make_addr(view_t<address> v) {
  return address::v6(v.data().data());
};

} // namespace <anonymous>

data make_data(data_view x) {
  return caf::visit(detail::overload(
    [](view_t<caf::none_t> v) { return data{v}; },
    [](view_t<boolean> v) { return data{v}; },
    [](view_t<integer> v) { return data{v}; },
    [](view_t<count> v) { return data{v}; },
    [](view_t<real> v) { return data{v}; },
    [](view_t<timespan> v) { return data{v}; },
    [](view_t<timestamp> v) { return data{v}; },
    [](view_t<std::string> v) { return data{std::string{v}}; },
    [](view_t<pattern> v) { return data{pattern{std::string{v.string()}}}; },
    [](view_t<address> v) { return data{make_addr(v)}; },
    [](view_t<subnet> v) {
      return data{subnet{make_addr(v.network()), v.length()}};
    },
    [](view_t<port> v) { return data{v}; },
    [](view_t<vector> v) {
      vector xs;
      xs.reserve(v->size());
      std::transform(v->begin(), v->end(), std::back_inserter(xs),
                     [](auto y) { return make_data(y); });
      return data{std::move(xs)};
    },
    [](view_t<set> v) {
      set xs;
      std::transform(v->begin(), v->end(), std::inserter(xs, xs.end()),
                     [](auto y) { return make_data(y); });
      return data{std::move(xs)};
    },
    [](view_t<map> v) {
      map xs;
      auto make = [](auto pair) {
        return std::make_pair(make_data(pair.first), make_data(pair.second));
      };
      std::transform(v->begin(), v->end(), std::inserter(xs, xs.end()), make);
      return data{std::move(xs)};
    }
  ), x);
}

// -- materialization ----------------------------------------------------------

std::string materialize(std::string_view x) {
  return std::string{x};
}

pattern materialize(pattern_view x) {
  return pattern{std::string{x.string()}};
}

address materialize(address_view x) {
  return address{x.data()};
}

subnet materialize(subnet_view x) {
  return subnet{materialize(x.network()), x.length()};
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
