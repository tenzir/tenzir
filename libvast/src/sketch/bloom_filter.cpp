//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/bloom_filter.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/worm.hpp"
#include "vast/error.hpp"
#include "vast/fbs/bloom_filter.hpp"

#include <fmt/format.h>

namespace vast {
namespace detail {
namespace {

void add(uint64_t digest, std::span<uint64_t> bits, size_t k,
         size_t m) noexcept {
  for (size_t i = 0; i < k; ++i) {
    auto idx = worm64(m, digest);
    bits[idx >> 6] |= (uint64_t{1} << (idx & 63));
  }
}

bool lookup(uint64_t digest, std::span<const uint64_t> bits, size_t k,
            size_t m) noexcept {
  for (size_t i = 0; i < k; ++i) {
    auto idx = worm64(m, digest);
    if ((bits[idx >> 6] & (uint64_t{1} << (idx & 63))) == 0)
      return false;
  }
  return true;
}

} // namespace
} // namespace detail

namespace sketch {

caf::expected<bloom_filter> bloom_filter::make(bloom_filter_parameters xs) {
  if (auto ys = evaluate(xs)) {
    if (*ys->k == 0)
      return caf::make_error(ec::invalid_argument, "need >= 1 hash functions");
    if (*ys->m == 0)
      return caf::make_error(ec::invalid_argument, "size cannot be 0");
    // Make m odd for worm hashing to be regenerative.
    // TODO: move this into evaluate.
    auto m = *ys->m;
    m = m - ~(m & 1);
    *ys->m = m;
    return bloom_filter{*ys};
  }
  return caf::make_error(ec::invalid_argument, "failed to evaluate parameters");
}

void bloom_filter::add(uint64_t digest) noexcept {
  detail::add(digest, bits_, *params_.k, *params_.m);
}

bool bloom_filter::lookup(uint64_t digest) const noexcept {
  return detail::lookup(digest, bits_, *params_.k, *params_.m);
}

const bloom_filter_parameters& bloom_filter::parameters() const noexcept {
  return params_;
}

size_t mem_usage(const bloom_filter& x) {
  return sizeof(x.params_) + sizeof(x.bits_) + x.bits_.size() * 8;
}

caf::expected<frozen_bloom_filter> freeze(const bloom_filter& x) {
  constexpr auto fixed_size = 56;
  const auto expected_size = fixed_size + x.bits_.size() * sizeof(uint64_t);
  // FlatBuffers <= 1.11 does not correctly use '::flatbuffers::soffset_t' over
  // 'soffset_t' in FLATBUFFERS_MAX_BUFFER_SIZE.
  using ::flatbuffers::soffset_t;
  if (expected_size >= FLATBUFFERS_MAX_BUFFER_SIZE)
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("frozen size {} exceeds max flatbuffer size of {} bytes",
                  expected_size, FLATBUFFERS_MAX_BUFFER_SIZE));
  // We know the exact size, so we reserve it to avoid re-allocations.
  flatbuffers::FlatBufferBuilder builder{expected_size};
  auto params = x.parameters();
  auto flat_params
    = fbs::bloom_filter::Parameters{*params.m, *params.n, *params.k, *params.p};
  auto bits_offset = builder.CreateVector(x.bits_);
  auto bloom_filter_offset
    = fbs::bloom_filter::Createv0(builder, &flat_params, bits_offset);
  builder.Finish(bloom_filter_offset);
  auto result = builder.Release();
  VAST_ASSERT(result.size() == expected_size);
  auto table = chunk::make(std::move(result));
  return frozen_bloom_filter{std::move(table)};
}

bloom_filter::bloom_filter(bloom_filter_parameters params) : params_{params} {
  bits_.resize((*params.m + 63) / 64); // integer ceiling
  std::fill(bits_.begin(), bits_.end(), 0);
}

frozen_bloom_filter::frozen_bloom_filter(chunk_ptr table) noexcept
  : table_{std::move(table)} {
  VAST_ASSERT(table_ != nullptr);
}

bool frozen_bloom_filter::lookup(uint64_t digest) const noexcept {
  // TODO: measure potential overhead of pointer chasing.
  auto table = fbs::bloom_filter::Getv0(table_->data());
  auto bits = std::span{table->bits()->data(), table->bits()->size()};
  return detail::lookup(digest, bits, table->parameters()->k(),
                        table->parameters()->m());
}

bloom_filter_parameters frozen_bloom_filter::parameters() const noexcept {
  bloom_filter_parameters result;
  auto table = fbs::bloom_filter::Getv0(table_->data());
  result.m = table->parameters()->m();
  result.n = table->parameters()->n();
  result.k = table->parameters()->k();
  result.p = table->parameters()->p();
  return result;
}

size_t mem_usage(const frozen_bloom_filter& x) noexcept {
  return x.table_->size();
}

} // namespace sketch
} // namespace vast
