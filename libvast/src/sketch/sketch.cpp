//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/sketch.hpp"

#include "vast/data.hpp"
#include "vast/die.hpp"
#include "vast/fbs/sketch.hpp"
#include "vast/hash/hash.hpp"
#include "vast/operator.hpp"
#include "vast/sketch/bloom_filter_view.hpp"

namespace vast::sketch {

sketch::sketch(chunk_ptr flatbuffer) noexcept
  : flatbuffer_{std::move(flatbuffer)} {
  VAST_ASSERT(flatbuffer_);
}

std::optional<bool>
sketch::lookup(relational_operator op, const data& x) const noexcept {
  auto root = fbs::GetSketch(flatbuffer_->data());
  switch (root->sketch_type()) {
    case fbs::sketch::Sketch::NONE: {
      die("sketch type must not be NONE");
    }
    case fbs::sketch::Sketch::bloom_filter_v0: {
      // TODO: also consider `X in [a,b,c]`.
      if (op != relational_operator::equal)
        return {};
      immutable_bloom_filter_view view;
      auto v0 = root->sketch_as_bloom_filter_v0();
      auto err = unpack(*v0->bloom_filter(), view);
      VAST_ASSERT(!err);
      // FIXME: Hash data exactly as we've done in the builder.
      auto h = [](const auto& x) {
        return hash(x);
      };
      auto digest = caf::visit(h, x);
      return view.lookup(digest);
    }
  }
  return {};
}

size_t mem_usage(const sketch& x) {
  return x.flatbuffer_->size();
}

} // namespace vast::sketch
