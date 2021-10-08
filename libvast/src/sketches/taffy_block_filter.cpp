//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketches/taffy_block_filter.hpp"

#include "vast/detail/assert.hpp"

#include <filter/taffy-block.hpp>

namespace vast {

struct taffy_block_filter::impl {
  impl(uint64_t n, double p)
    : filter{filter::TaffyBlockFilter::CreateWithNdvFpp(n, p)} {
    VAST_ASSERT(n > 0);
    VAST_ASSERT(0 < p && p < 1);
  }

  filter::TaffyBlockFilter filter;
};

taffy_block_filter::~taffy_block_filter() {
  // defined to satisfy unique-ptr PIMPL
}

taffy_block_filter::taffy_block_filter(uint64_t n, double p)
  : impl_{std::make_unique<impl>(n, p)} {
}

void taffy_block_filter::add(digest_type x) {
  impl_->filter.InsertHash(x);
}

bool taffy_block_filter::lookup(digest_type x) {
  return impl_->filter.FindHash(x);
}

} // namespace vast
