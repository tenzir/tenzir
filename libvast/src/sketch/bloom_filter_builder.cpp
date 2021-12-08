//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/bloom_filter_builder.hpp"

#include "vast/error.hpp"
#include "vast/fbs/bloom_filter.hpp"
#include "vast/fbs/probabilistic_filter.hpp"
#include "vast/fbs/sketch.hpp"
#include "vast/sketch/bloom_filter.hpp"
#include "vast/sketch/sketch.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <flatbuffers/flatbuffers.h>

#include <cstdint>
#include <unordered_set>

namespace vast::sketch {

bloom_filter_builder::bloom_filter_builder(double p) : p_{p} {
}

caf::expected<sketch>
bloom_filter_builder::build(const std::unordered_set<uint64_t>& digests) {
  // Compute optimal m using n and p.
  bloom_filter_config config;
  config.n = digests.size();
  config.p = p_;
  auto params = evaluate(config);
  if (!params)
    return caf::make_error(ec::invalid_argument, //
                           "invalid Bloom filter parameters");
  // Estimate the size.
  const auto bitvector_size = (params->m + 63) / 64;
  constexpr auto flatbuffer_size = 42; // TODO: compute size manually
  flatbuffers::FlatBufferBuilder builder{flatbuffer_size};
  // Create a Bloom filter embedded in the builder allocator.
  uint64_t* buf;
  auto bits_offset = builder.CreateUninitializedVector(bitvector_size, &buf);
  std::fill_n(buf, bitvector_size, 0);
  auto bits = std::span{buf, bitvector_size};
  auto view = mutable_bloom_filter_view{*params, bits};
  for (auto digest : digests)
    view.add(digest);
  // Pack the flatbuffer tables.
  auto flat_params
    = fbs::BloomFilterParameters{params->m, params->n, params->k, params->p};
  auto bloom_filter_offset
    = fbs::CreateBloomFilter(builder, &flat_params, bits_offset);
  auto bloom_filter_v0_offset
    = fbs::sketch::bloom_filter::Createv0(builder, bloom_filter_offset);
  auto sketch_offset
    = fbs::CreateSketch(builder, fbs::sketch::Sketch::bloom_filter_v0,
                        bloom_filter_v0_offset.Union());
  builder.Finish(sketch_offset);
  // TODO: verify actual size
  // VAST_ASSERT(builder.GetSize() == flatbuffer_size);
  auto fb = flatbuffer<fbs::Sketch>::make(builder.Release());
  if (!fb)
    return fb.error();
  return sketch{std::move(*fb)};
}

} // namespace vast::sketch
