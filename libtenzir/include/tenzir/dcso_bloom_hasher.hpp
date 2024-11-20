//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/as_bytes.hpp>
#include <tenzir/concept/printable/tenzir/view.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/operators.hpp>
#include <tenzir/detail/type_traits.hpp>
#include <tenzir/hash/hasher.hpp>
#include <tenzir/view.hpp>

#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>

namespace tenzir {

template <class HashFunction>
class dcso_bloom_hasher
  : public hasher<dcso_bloom_hasher<HashFunction>, uint64_t>,
    detail::equality_comparable<dcso_bloom_hasher<HashFunction>> {
  using super = hasher<dcso_bloom_hasher<HashFunction>, uint64_t>;

public:
  // Directly taken from DCSO's bloom.
  static constexpr uint64_t m = 18446744073709551557ull;

  // Directly taken from DCSO's bloom.
  static constexpr uint64_t g = 18446744073709550147ull;

  /// Constructs a DCSO bloom hasher.
  /// @param k The number of hash digests to compute.
  /// @pre `k > 0`
  explicit dcso_bloom_hasher(size_t k) : super{k} {
  }

  /// Computes *k* hash digests.
  /// We're doing the same calculation as DCSO's `Fingerprint` function here,
  /// except that we don't do the final "mod filter cells" because our Bloom
  /// filter implementation does that for us.
  template <class Ts>
  auto hash(std::span<const std::byte> bytes, Ts& xs) -> void {
    HashFunction h;
    h(bytes.data(), bytes.size());
    auto digest = static_cast<uint64_t>(h);
    digest %= m;
    for (size_t i = 0; i < xs.size(); ++i) {
      digest = (digest * g) % m;
      // Unlike DCSO's version, we don't do a modulo-number-of-cells operation
      // when we assign the fingerprint becaues our Bloom filter implementation
      // does this later.
      xs[i] = digest;
    }
  }

  /// Computes *k* hash digests over anything that can be interpreted as bytes.
  template <class T, class Ts>
  auto hash(const T& x, Ts& xs) -> void {
    hash(as_bytes(x), xs);
  }

  template <class Ts>
  auto hash(view<data> x, Ts& xs) -> void {
    // DCSO's bloom can only handle strings, so everything that's not a string
    // needs to be converted here.
    auto h = [this, &xs](auto&& value) {
      this->hash(value, xs);
    };
    auto f = [&](const auto& value) {
      using namespace std::string_view_literals;
      using view_type = std::decay_t<decltype(value)>;
      if constexpr (std::is_same_v<view_type, caf::none_t>) {
        h(""sv);
      } else if constexpr (std::is_same_v<view_type, view<bool>>) {
        h(value ? "true"sv : "false"sv);
      } else if constexpr (detail::is_any_v<view_type, view<std::string>,
                                            view<blob>>) {
        h(value);
      } else if constexpr (detail::is_any_v<view_type, view<pattern>>) {
        h(value.string());
      } else if constexpr (detail::is_any_v<view_type, view<list>, view<map>,
                                            view<record>>) {
        // For compound values, we assume that users provide values in JSON to
        // Bloom.
        auto json = to_json(materialize(value));
        TENZIR_ASSERT(json);
        h(*json);
      } else if constexpr (detail::is_any_v<view_type, view<int64_t>,
                                            view<uint64_t>, view<double>,
                                            view<duration>, view<time>, view<ip>,
                                            view<subnet>, view<enumeration>>) {
        // By default, we convert to string. For cases that may have multiple
        // representations, such as durations, we need to have a dialogue with
        // our users before committing to a fixed string representation.
        h(to_string(value));
      } else {
        static_assert(detail::always_false_v<view_type>,
                      "missing type dispatch");
      }
    };
    match(x, f);
  }

  template <class Ts>
  auto hash(const data& x, Ts& xs) -> void {
    hash(make_view(x), xs);
  }

  friend auto operator==(const dcso_bloom_hasher&, const dcso_bloom_hasher&)
    -> bool {
    return true; // stateless
  }

  friend auto inspect(auto& f, dcso_bloom_hasher& x) {
    return f.object(x)
      .pretty_name("dcso_bloom_hasher")
      .fields(f.field("value", static_cast<super&>(x)));
  }
};

} // namespace tenzir
