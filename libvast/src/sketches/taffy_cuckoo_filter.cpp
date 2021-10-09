//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketches/taffy_cuckoo_filter.hpp"

#include "vast/detail/assert.hpp"

#include <filter/taffy-cuckoo.hpp>

namespace vast {

struct taffy_cuckoo_filter::impl {
  impl(size_t m) : filter{filter::TaffyCuckooFilter::CreateWithBytes(m)} {
  }

  filter::TaffyCuckooFilter filter;
};

taffy_cuckoo_filter::~taffy_cuckoo_filter() {
  // defined to satisfy unique-ptr PIMPL
}

taffy_cuckoo_filter::taffy_cuckoo_filter(size_t m)
  : impl_{std::make_unique<impl>(m)} {
}

void taffy_cuckoo_filter::add(digest_type x) {
  impl_->filter.InsertHash(x);
}

bool taffy_cuckoo_filter::lookup(digest_type x) {
  return impl_->filter.FindHash(x);
}

} // namespace vast
