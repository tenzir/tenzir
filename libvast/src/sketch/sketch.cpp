//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/sketch.hpp"

#include "vast/data.hpp"
#include "vast/detail/hash_scalar.hpp"
#include "vast/die.hpp"
#include "vast/fbs/sketch.hpp"
#include "vast/hash/hash.hpp"
#include "vast/operator.hpp"
#include "vast/sketch/bloom_filter_view.hpp"
#include "vast/time_synopsis.hpp"
#include "vast/view.hpp"

namespace vast::sketch {

namespace {

/// Checks whether the hash digest(s) exist according to a provided lookup
/// function.
std::optional<bool> hash_lookup(const type& t, const data& x, auto lookup) {
  if (caf::holds_alternative<caf::none_t>(x))
    return lookup(detail::nil_hash_digest);
  if (!t)
    return std::nullopt;
  auto f = detail::overload{
    [&]<basic_type T>(const T&) -> std::optional<bool> {
      const auto& concrete = caf::get<type_to_data_t<T>>(x);
      return lookup(detail::hash_scalar<T>(make_view(concrete)));
    },
    [&](const enumeration_type&) -> std::optional<bool> {
      VAST_ASSERT(false, "enum should not be an inferred type");
      return std::nullopt;
    },
    [&](const list_type& t) -> std::optional<bool> {
      /// Check if any value is part of the list.
      auto num_false = size_t{0};
      const auto& xs = caf::get<list>(x);
      for (auto&& elem : xs) {
        if (auto result = hash_lookup(t.value_type(), elem, lookup)) {
          if (*result)
            return true;
          ++num_false;
        }
      }
      // Only if all values are "false", the list doesn't qualify.
      if (num_false == xs.size())
        return false;
      return std::nullopt;
    },
    [&](const map_type& t) -> std::optional<bool> {
      /// Check if any value is part of the map, either as key or value.
      auto num_false = size_t{0};
      const auto& xs = caf::get<map>(x);
      for (auto&& [k, v] : xs) {
        if (auto result = hash_lookup(t.key_type(), k, lookup)) {
          if (*result)
            return true;
          ++num_false;
        }
        if (auto result = hash_lookup(t.value_type(), v, lookup)) {
          if (*result)
            return true;
          ++num_false;
        }
      }
      // Only if all keys and values are "false", the map doesn't qualify.
      if (num_false == xs.size() * 2)
        return false;
      return std::nullopt;
    },
    [&](const record_type&) -> std::optional<bool> {
      // A record is a product type, so all values must be present.
      // FIXME: implement
      return std::nullopt;
    },
  };
  return caf::visit(f, t);
}

} // namespace

sketch::sketch(flatbuffer<fbs::Sketch> fb) noexcept
  : flatbuffer_{std::move(fb)} {
}

std::optional<bool>
sketch::lookup(relational_operator op, const data& x) const noexcept {
  switch (flatbuffer_->sketch_type()) {
    case fbs::sketch::Sketch::NONE: {
      die("sketch type must not be NONE");
    }
    // FIXME: Expose the logic of the old time synopsis so that we can
    // call it directly with any data type here.
    case fbs::sketch::Sketch::min_max_u64: {
      auto const* time = flatbuffer_->sketch_as_min_max_u64();
      auto syn = vast::time_synopsis{vast::time{} + duration{time->min()},
                                     vast::time{} + duration{time->max()}};
      return syn.lookup(op, make_view(x));
    }
    case fbs::sketch::Sketch::min_max_f64: {
      auto const* time = flatbuffer_->sketch_as_min_max_f64();
      auto syn = vast::time_synopsis{
        vast::time{} + duration{static_cast<int64_t>(time->min())},
        vast::time{} + duration{static_cast<int64_t>(time->max())}};
      return syn.lookup(op, make_view(x));
    }

    case fbs::sketch::Sketch::min_max_i64: {
      auto const* time = flatbuffer_->sketch_as_min_max_i64();
      auto syn = vast::time_synopsis{vast::time{} + duration{time->min()},
                                     vast::time{} + duration{time->max()}};
      return syn.lookup(op, make_view(x));
    }
    case fbs::sketch::Sketch::bloom_filter: {
      // TODO: also consider `X in [a,b,c]`.
      if (op != relational_operator::equal)
        return {};
      immutable_bloom_filter_view bf;
      auto err = unpack(*flatbuffer_->sketch_as_bloom_filter(), bf);
      VAST_ASSERT(!err);
      auto f = [&](uint64_t digest) {
        return bf.lookup(digest);
      };
      return hash_lookup(type::infer(x), x, f);
    }
  }
  return {};
}

size_t mem_usage(const sketch& x) {
  return x.flatbuffer_.chunk()->size();
}

flatbuffers::Offset<flatbuffers::Vector<uint8_t>>
pack_nested(flatbuffers::FlatBufferBuilder& builder, const sketch& sketch) {
  auto sketch_bytes = as_bytes(sketch.flatbuffer_.chunk());
  return builder.CreateVector(
    reinterpret_cast<const uint8_t*>(sketch_bytes.data()), sketch_bytes.size());
}

} // namespace vast::sketch
