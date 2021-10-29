//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/bloom_filter.hpp"

#include "vast/detail/worm.hpp"
#include "vast/error.hpp"

namespace vast::sketch {

caf::expected<bloom_filter> bloom_filter::make(bloom_filter_parameters xs) {
  if (auto ys = evaluate(xs)) {
    if (*ys->k == 0)
      return caf::make_error(ec::invalid_argument, "need >= 1 hash functions");
    if (*ys->m == 0)
      return caf::make_error(ec::invalid_argument, "size cannot be 0");
    // Make m odd for worm hashing to be regenerative.
    auto m = *ys->m;
    m = m - ~(m & 1);
    *ys->m = m;
    return bloom_filter{*ys};
  }
  return caf::make_error(ec::invalid_argument, "failed to evaluate parameters");
}

void bloom_filter::add(uint64_t digest) noexcept {
  for (size_t i = 0; i < *params_.k; ++i) {
    auto idx = detail::worm64(*params_.m, digest);
    bits_[idx >> 6] |= (uint64_t{1} << (idx & 63));
  }
}

bool bloom_filter::lookup(uint64_t digest) const noexcept {
  for (size_t i = 0; i < *params_.k; ++i) {
    auto idx = detail::worm64(*params_.m, digest);
    if ((bits_[idx >> 6] & (uint64_t{1} << (idx & 63))) == 0)
      return false;
  }
  return true;
}

const bloom_filter_parameters& bloom_filter::parameters() const noexcept {
  return params_;
}

size_t mem_usage(const bloom_filter& x) {
  return sizeof(x.params_) + sizeof(x.bits_) + x.bits_.size() * 8;
}

// caf::expected<frozen_bloom_filter> freeze(const bloom_filter const& x) {
//   // TODO: make sure the Bloom filter fits in the Flatbuffer.
//   flatbuffers::FlatBufferBuilder builder;
//   auto parameters_offset = fbs::bloom_filter::CreateParameters(
//     builder, params.m, params.n, params.k, params.p);
//   auto bits_offset = builder.CreateVector(bits_.data(), bits_.size());
//   auto bloom_filter_offset
//     = fbs::bloom_filter::Createv0(builder, parameters_offset, bits_offset);
//   builder.Finish(bloom_filter_offset);
//   auto result = builder.Release();
//   flatbuffer_ = chunk::make(std::move(result));
// }

bloom_filter::bloom_filter(bloom_filter_parameters params) : params_{params} {
  bits_.resize((*params.m + 63) / 64); // integer ceiling
  std::fill(bits_.begin(), bits_.end(), 0);
}

} // namespace vast::sketch
