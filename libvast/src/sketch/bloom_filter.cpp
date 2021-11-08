//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/bloom_filter.hpp"

#include "vast/fbs/bloom_filter.hpp"

#include <fmt/format.h>

namespace vast::sketch {
namespace detail {

caf::expected<std::pair<flatbuffers::DetachedBuffer, bloom_filter_view<uint64_t>>>
make_uninitialized(bloom_filter_params params) {
  VAST_ASSERT(params.m > 0);
  VAST_ASSERT(params.m & 1);
  constexpr auto fixed_size = 56;
  const auto bitvector_size = (params.m + 63) / 64;
  const auto expected_size = fixed_size + bitvector_size * sizeof(uint64_t);
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
  auto flat_params
    = fbs::BloomFilterParameters{params.m, params.n, params.k, params.p};
  uint64_t* buf;
  auto bits_offset = builder.CreateUninitializedVector(bitvector_size, &buf);
  auto bloom_filter_offset
    = fbs::CreateBloomFilter(builder, &flat_params, bits_offset);
  builder.Finish(bloom_filter_offset);
  auto buffer = builder.Release();
  VAST_ASSERT(buffer.size() == expected_size);
  auto root = fbs::GetMutableBloomFilter(buffer.data());
  bloom_filter_view<uint64_t> view;
  view.params = params;
  auto bits = root->mutable_bits();
  view.bits = std::span{bits->data(), bits->size()};
  return std::pair{std::move(buffer), view};
}

} // namespace detail

frozen_bloom_filter::frozen_bloom_filter(chunk_ptr table) noexcept
  : table_{std::move(table)} {
  VAST_ASSERT(table_ != nullptr);
  auto root = fbs::GetBloomFilter(table_->data());
  view_.params.m = root->parameters()->m();
  view_.params.n = root->parameters()->n();
  view_.params.k = root->parameters()->k();
  view_.params.p = root->parameters()->p();
  view_.bits = std::span{root->bits()->data(), root->bits()->size()};
}

bool frozen_bloom_filter::lookup(uint64_t digest) const noexcept {
  return view_.lookup(digest);
}

bloom_filter_params frozen_bloom_filter::parameters() const noexcept {
  return view_.params;
}

size_t mem_usage(const frozen_bloom_filter& x) noexcept {
  return mem_usage(x.view_), x.table_->size();
}

caf::expected<bloom_filter> bloom_filter::make(bloom_filter_config cfg) {
  if (auto params = evaluate(cfg)) {
    if (params->k == 0)
      return caf::make_error(ec::invalid_argument, "need >= 1 hash functions");
    if (params->m == 0)
      return caf::make_error(ec::invalid_argument, "size cannot be 0");
    return bloom_filter{*params};
  }
  return caf::make_error(ec::invalid_argument, "failed to evaluate parameters");
}

void bloom_filter::add(uint64_t digest) noexcept {
  view_.add(digest);
}

bool bloom_filter::lookup(uint64_t digest) const noexcept {
  return view_.lookup(digest);
}

const bloom_filter_params& bloom_filter::parameters() const noexcept {
  return view_.params;
}

size_t mem_usage(const bloom_filter& x) {
  return mem_usage(x.view_) + sizeof(x.bits_) + x.bits_.size() * 8;
}

caf::expected<frozen_bloom_filter> freeze(const bloom_filter& x) {
  auto pair = detail::make_uninitialized(x.parameters());
  if (!pair)
    return pair.error();
  auto& view = pair->second;
  VAST_ASSERT(x.view_.bits.size() == view.bits.size());
  std::copy(x.view_.bits.begin(), x.view_.bits.end(), view.bits.begin());
  return frozen_bloom_filter{chunk::make(std::move(pair->first))};
}

bloom_filter::bloom_filter(bloom_filter_params params) {
  // Make m odd for worm hashing to be regenerative.
  params.m -= ~(params.m & 1);
  bits_.resize((params.m + 63) / 64); // integer ceiling
  std::fill(bits_.begin(), bits_.end(), 0);
  view_.params = params;
  view_.bits = std::span{bits_.data(), bits_.size()};
}

} // namespace vast::sketch
