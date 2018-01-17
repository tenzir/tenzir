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

#include "vast/uuid.hpp"

#include <random>

namespace vast {
namespace {

class random_generator {
  using number_type = unsigned long;
  using distribution = std::uniform_int_distribution<number_type>;

public:
  random_generator()
    : unif_{std::numeric_limits<number_type>::min(),
            std::numeric_limits<number_type>::max()} {
  }

  uuid operator()() {
    uuid u;
    auto r = unif_(rd_);
    int i = 0;
    for (auto& byte : u) {
      if (i == sizeof(number_type)) {
        r = unif_(rd_);
        i = 0;
      }
      byte = (r >> (i * 8)) & 0xff;
      ++i;
    }
    // Set variant to 0b10xxxxxx.
    u[8] &= 0xbf;
    u[8] |= 0x80;
    // Set version to 0b0100xxxx.
    u[6] &= 0x4f; // 0b01001111
    u[6] |= 0x40; // 0b01000000
    return u;
  }

private:
  std::random_device rd_;
  distribution unif_;
};

} // namespace <anonymous>

uuid uuid::random() {
  return random_generator{}();
}

uuid uuid::nil() {
  uuid u;
  u.id_.fill(0);
  return u;
}

uuid::reference uuid::operator[](size_t i) {
  return id_[i];
}

uuid::const_reference uuid::operator[](size_t i) const {
  return id_[i];
}

uuid::iterator uuid::begin() {
  return id_.begin();
}

uuid::iterator uuid::end() {
  return id_.end();
}

uuid::const_iterator uuid::begin() const {
  return id_.begin();
}

uuid::const_iterator uuid::end() const {
  return id_.end();
}

uuid::size_type uuid::size() const {
  return 16;
}

void uuid::swap(uuid& other) {
  std::swap_ranges(begin(), end(), other.begin());
}

bool operator==(uuid const& x, uuid const& y) {
  return std::equal(x.begin(), x.end(), y.begin());
}

bool operator<(uuid const& x, uuid const& y) {
  return std::lexicographical_compare(x.begin(), x.end(), y.begin(), y.end());
}

} // namespace vast
