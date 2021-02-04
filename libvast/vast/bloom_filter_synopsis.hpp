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
#include "vast/fbs/synopsis.hpp"
#include "vast/synopsis.hpp"
#include "vast/type.hpp"

#include <caf/deserializer.hpp>
#include <caf/optional.hpp>
#include <caf/serializer.hpp>

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

  bloom_filter_synopsis(vast::type x, const vast::fbs::bloom_synopsis::v0* fb)
    : synopsis(std::move(x)) {
    span<const uint64_t> sp(fb->data()->data(), fb->data()->size());
    bloom_filter<HashFunction, double_hasher, policy::no_partitioning,
                 detail::mms::memory_view>
      bf(fb->size(), sp);
    bloom_filter_ = bf.make_standalone();
  }

  void add(data_view x) override {
    bloom_filter_.add(caf::get<view<T>>(x));
  }

  caf::optional<bool>
  lookup(relational_operator op, data_view rhs) const override {
    switch (op) {
      default:
        return caf::none;
      case relational_operator::equal:
        return bloom_filter_.lookup(caf::get<view<T>>(rhs));
      case relational_operator::in: {
        if (auto xs = caf::get_if<view<list>>(&rhs)) {
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

  size_t memusage() const override {
    return bloom_filter_.memusage();
  }

  caf::error serialize(caf::serializer& sink) const override {
    return sink(bloom_filter_);
  }

  caf::error deserialize(caf::deserializer& source) override {
    return source(bloom_filter_);
  }

protected:
  // This is the current termination point for the `mms` system, i.e. we always
  // create a standalone version when creating a synopsis. In the future we
  // probably want to lift this higher, so we can use the whole partition
  // synopsis straight from disk, without deserialization, but it doesn't make
  // much sense on the individual synopsis level because it typically only
  // occupies a small part of the buffer.
  bloom_filter<HashFunction, double_hasher, policy::no_partitioning,
               detail::mms::standalone>
    bloom_filter_;
};

// Because VAST deserializes a synopsis with empty options and
// construction of an address synopsis fails without any sizing
// information, we augment the type with the synopsis options.

/// Creates a new type annotation from a set of bloom filter parameters.
/// @returns The provided type with a new `#synopsis=bloom_filter(n,p)`
///          attribute. Note that all previous attributes are discarded.
type annotate_parameters(type type, const bloom_filter_parameters& params);

/// Parses Bloom filter parameters from type attributes of the form
/// `#synopsis=bloom_filter(n,p)`.
/// @param x The type whose attributes to parse.
/// @returns The parsed and evaluated Bloom filter parameters.
/// @relates bloom_filter_synopsis
caf::optional<bloom_filter_parameters> parse_parameters(const type& x);

// caf::error unpack(const fbs::synopsis::v0&, synopsis_ptr&);

} // namespace vast
