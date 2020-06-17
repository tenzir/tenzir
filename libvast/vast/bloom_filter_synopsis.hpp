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

#pragma once

#include "vast/bloom_filter.hpp"

#include <caf/deserializer.hpp>
#include <caf/optional.hpp>
#include <caf/serializer.hpp>

#include <vast/detail/assert.hpp>
#include <vast/synopsis.hpp>
#include <vast/type.hpp>

namespace vast {

/// A Bloom filter synopsis.
template <class T, class HashFunction>
class bloom_filter_synopsis : public synopsis {
public:
  using bloom_filter_type = bloom_filter<HashFunction>;
  using hasher_type = typename bloom_filter_type::hasher_type;

  bloom_filter_synopsis(vast::type x, bloom_filter_type bf)
    : synopsis{std::move(x)}, bloom_filter_{std::move(bf)} {
    // nop
  }

  void add(data_view x) override {
    bloom_filter_.add(caf::get<view<T>>(x));
  }

  caf::optional<bool>
  lookup(relational_operator op, data_view rhs) const override {
    switch (op) {
      default:
        return caf::none;
      case equal:
        return bloom_filter_.lookup(caf::get<view<T>>(rhs));
      case in: {
        if (auto xs = caf::get_if<view<set>>(&rhs)) {
          for (auto x : **xs)
            if (bloom_filter_.lookup(caf::get<view<T>>(x)))
              return true;
          return false;
        }
        return caf::none;
      }
    }
  }

  bool equals(const synopsis& other) const noexcept override {
    if (typeid(other) != typeid(bloom_filter_synopsis))
      return false;
    auto& rhs = static_cast<const bloom_filter_synopsis&>(other);
    return this->type() == rhs.type() && bloom_filter_ == rhs.bloom_filter_;
  }

  caf::error serialize(caf::serializer& sink) const override {
    return sink(bloom_filter_);
  }

  caf::error deserialize(caf::deserializer& source) override {
    return source(bloom_filter_);
  }

protected:
  bloom_filter<HashFunction> bloom_filter_;
};

/// Parses Bloom filter parameters from type attributes of the form
/// `#synopsis=bloom_filter(n,p)`.
/// @param x The type whose attributes to parse.
/// @returns The parsed and evaluated Bloom filter parameters.
/// @relates bloom_filter_synopsis
caf::optional<bloom_filter_parameters> parse_parameters(const type& x);

} // namespace vast
