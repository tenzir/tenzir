//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/bloom_filter.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/type.hpp"

#include <optional>

namespace tenzir {

/// A Bloom filter synopsis.
template <class T, class HashFunction>
class bloom_filter_synopsis : public synopsis {
public:
  using bloom_filter_type = bloom_filter<HashFunction>;
  using hasher_type = typename bloom_filter_type::hasher_type;

  bloom_filter_synopsis(tenzir::type x, bloom_filter_type bf)
    : synopsis{std::move(x)}, bloom_filter_{std::move(bf)} {
    // nop
  }

  [[nodiscard]] synopsis_ptr clone() const override {
    using self = bloom_filter_synopsis<T, HashFunction>;
    return std::make_unique<self>(type(), bloom_filter_);
  }

  void add(data_view x) override {
    TENZIR_ASSERT(caf::holds_alternative<view<T>>(x), "invalid data");
    bloom_filter_.add(as<view<T>>(x));
  }

  [[nodiscard]] std::optional<bool>
  lookup(relational_operator op, data_view rhs) const override {
    switch (op) {
      default:
        return {};
      case relational_operator::equal:
        // TODO: We should treat 'null' as a normal value and
        // hash it, so we can exclude synopsis where all values
        // are non-null.
        if (caf::holds_alternative<view<caf::none_t>>(rhs))
          return {};
        if constexpr (std::is_same_v<T, std::string>) {
          if (caf::holds_alternative<view<pattern>>(rhs))
            return {};
        }
        if (!caf::holds_alternative<view<T>>(rhs))
          return false;
        return bloom_filter_.lookup(as<view<T>>(rhs));
      case relational_operator::in: {
        if (auto xs = caf::get_if<view<list>>(&rhs)) {
          for (auto x : **xs) {
            if (caf::holds_alternative<view<caf::none_t>>(x))
              return {};
            if (!caf::holds_alternative<view<T>>(x))
              continue;
            if (bloom_filter_.lookup(as<view<T>>(x))) {
              return true;
            }
          }
          return false;
        }
        return {};
      }
    }
  }

  [[nodiscard]] bool equals(const synopsis& other) const noexcept override {
    if (typeid(other) != typeid(bloom_filter_synopsis))
      return false;
    auto& rhs = static_cast<const bloom_filter_synopsis&>(other);
    return this->type() == rhs.type() && bloom_filter_ == rhs.bloom_filter_;
  }

  [[nodiscard]] size_t memusage() const override {
    return bloom_filter_.memusage();
  }

  bool inspect_impl(supported_inspectors& inspector) override {
    return std::visit(
      [this](auto inspector) {
        return inspector.get().apply(bloom_filter_);
      },
      inspector);
  }

  const bloom_filter<HashFunction>& filter() const {
    return bloom_filter_;
  }

protected:
  bloom_filter<HashFunction> bloom_filter_;
};

// Because Tenzir deserializes a synopsis with empty options and
// construction of an address synopsis fails without any sizing
// information, we augment the type with the synopsis options.

/// Creates a new type annotation from a set of bloom filter parameters.
/// @returns The provided type with a new `#synopsis=bloom_filter(n,p)`
///          attribute. Note that all previous attributes are discarded.
type annotate_parameters(const type& type,
                         const bloom_filter_parameters& params);

/// Parses Bloom filter parameters from type attributes of the form
/// `#synopsis=bloom_filter(n,p)`.
/// @param x The type whose attributes to parse.
/// @returns The parsed and evaluated Bloom filter parameters.
/// @relates bloom_filter_synopsis
std::optional<bloom_filter_parameters> parse_parameters(const type& x);

} // namespace tenzir
