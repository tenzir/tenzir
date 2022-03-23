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
#include "vast/view.hpp"

namespace vast::sketch {

sketch::sketch(flatbuffer<fbs::Sketch> fb) noexcept
  : flatbuffer_{std::move(fb)} {
}

std::optional<bool>
sketch::lookup(relational_operator op, const data& x) const noexcept {
  switch (flatbuffer_->sketch_type()) {
    case fbs::sketch::Sketch::NONE: {
      die("sketch type must not be NONE");
    }
    case fbs::sketch::Sketch::min_max: {
      auto min_max = flatbuffer_->sketch_as_min_max();
      // FIXME: implement the function such that it tests whether x falls into
      // the closed interval [min, max].
    }
    case fbs::sketch::Sketch::bloom_filter: {
      // TODO: also consider `X in [a,b,c]`.
      if (op != relational_operator::equal)
        return {};
      immutable_bloom_filter_view bf;
      auto err = unpack(*flatbuffer_->sketch_as_bloom_filter(), bf);
      VAST_ASSERT(!err);
      auto inferred_type = type::infer(x);
      auto h = detail::overload{
        [&]<basic_type T>(const T&) {
          const auto& concrete = caf::get<type_to_data_t<T>>(x);
          return bf.lookup(detail::hash_scalar<T>(make_view(concrete)));
        },
        [&]<complex_type T>(const T&) {
          // FIXME: perform disjunction of all values in complex types.
          return false;
        }};
      return caf::visit(h, inferred_type);
    }
  }
  return {};
}

size_t mem_usage(const sketch& x) {
  return x.flatbuffer_.chunk()->size();
}

} // namespace vast::sketch
