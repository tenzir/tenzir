/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/bitmap.hpp"

namespace vast {

bitmap::bitmap() : bitmap_{default_bitmap{}} {
}

bitmap::bitmap(size_type n, bool bit) : bitmap{} {
  append_bits(bit, n);
}

bool bitmap::empty() const {
  return caf::visit([](auto& bm) { return bm.empty(); }, bitmap_);
}

bitmap::size_type bitmap::size() const {
  return caf::visit([](auto& bm) { return bm.size(); }, bitmap_);
}

void bitmap::append_bit(bool bit) {
  caf::visit([=](auto& bm) { bm.append_bit(bit); }, bitmap_);
}

void bitmap::append_bits(bool bit, size_type n) {
  caf::visit([=](auto& bm) { bm.append_bits(bit, n); }, bitmap_);
}

void bitmap::append_block(block_type value, size_type n) {
  caf::visit([=](auto& bm) { bm.append_block(value, n); }, bitmap_);
}

void bitmap::flip() {
  caf::visit([](auto& bm) { bm.flip(); }, bitmap_);
}

bitmap::variant& bitmap::data() {
  return bitmap_;
}

const bitmap::variant& bitmap::data() const {
  return bitmap_;
}

bool operator==(const bitmap& x, const bitmap& y) {
  return x.bitmap_ == y.bitmap_;
}

bitmap_bit_range::bitmap_bit_range(const bitmap& bm) {
  auto visitor = [&](auto& b) {
    auto r = bit_range(b);
    if (!r.done())
      bits_ = r.get();
    range_ = std::move(r);
  };
  caf::visit(visitor, bm);
}

void bitmap_bit_range::next() {
  auto visitor = [&](auto& rng) {
    rng.next();
    bits_ = rng.get();
  };
  caf::visit(visitor, range_);
}

bool bitmap_bit_range::done() const {
  return caf::visit([](auto& rng) { return rng.done(); }, range_);
}

bitmap_bit_range bit_range(const bitmap& bm) {
  return bitmap_bit_range{bm};
}

} // namespace vast
