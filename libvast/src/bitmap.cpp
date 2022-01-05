//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/bitmap.hpp"

#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/fbs/bitmap.hpp"

namespace vast {

bitmap::bitmap() : bitmap_{default_bitmap{}} {
}

bitmap::bitmap(size_type n, bool bit) : bitmap{} {
  append_bits(bit, n);
}

bool bitmap::empty() const {
  return caf::visit(
    [](auto& bm) {
      return bm.empty();
    },
    bitmap_);
}

bitmap::size_type bitmap::size() const {
  return caf::visit(
    [](auto& bm) {
      return bm.size();
    },
    bitmap_);
}

size_t bitmap::memusage() const {
  return caf::visit(
    [](auto& bm) {
      return bm.memusage();
    },
    bitmap_);
}

void bitmap::append_bit(bool bit) {
  caf::visit(
    [=](auto& bm) {
      bm.append_bit(bit);
    },
    bitmap_);
}

void bitmap::append_bits(bool bit, size_type n) {
  caf::visit(
    [=](auto& bm) {
      bm.append_bits(bit, n);
    },
    bitmap_);
}

void bitmap::append_block(block_type value, size_type n) {
  caf::visit(
    [=](auto& bm) {
      bm.append_block(value, n);
    },
    bitmap_);
}

void bitmap::flip() {
  caf::visit(
    [](auto& bm) {
      bm.flip();
    },
    bitmap_);
}

bitmap::variant& bitmap::get_data() {
  return bitmap_;
}

const bitmap::variant& bitmap::get_data() const {
  return bitmap_;
}

bool operator==(const bitmap& x, const bitmap& y) {
  return x.bitmap_ == y.bitmap_;
}

auto pack(flatbuffers::FlatBufferBuilder& builder, const bitmap& from)
  -> flatbuffers::Offset<fbs::Bitmap> {
  auto f = detail::overload{
    [&](const ewah_bitmap& ewah) {
      const auto ewah_offset = pack(builder, ewah).Union();
      return fbs::CreateBitmap(builder, fbs::bitmap::Bitmap::ewah,
                               ewah_offset.Union());
    },
    [&](const null_bitmap& null) {
      const auto null_offset = pack(builder, null).Union();
      return fbs::CreateBitmap(builder, fbs::bitmap::Bitmap::null,
                               null_offset.Union());
    },
    [&](const wah_bitmap& wah) {
      const auto wah_offset = pack(builder, wah).Union();
      return fbs::CreateBitmap(builder, fbs::bitmap::Bitmap::wah,
                               wah_offset.Union());
    },
  };
  return caf::visit(f, from.bitmap_);
}

auto unpack(const fbs::Bitmap& from, bitmap& to) -> caf::error {
  switch (from.bitmap_type()) {
    case fbs::bitmap::Bitmap::NONE:
      return caf::make_error(ec::logic_error, "invalid vast.fbs.Bitmap type");
    case fbs::bitmap::Bitmap::ewah: {
      const auto& from_ewah = *from.bitmap_as_ewah();
      auto to_ewah = ewah_bitmap{};
      if (auto err = unpack(from_ewah, to_ewah))
        return err;
      to.bitmap_ = std::move(to_ewah);
      return caf::none;
    }
    case fbs::bitmap::Bitmap::null: {
      const auto& from_null = *from.bitmap_as_null();
      auto to_null = null_bitmap{};
      if (auto err = unpack(from_null, to_null))
        return err;
      to.bitmap_ = std::move(to_null);
      return caf::none;
    }
    case fbs::bitmap::Bitmap::wah: {
      const auto& from_wah = *from.bitmap_as_wah();
      auto to_wah = wah_bitmap{};
      if (auto err = unpack(from_wah, to_wah))
        return err;
      to.bitmap_ = std::move(to_wah);
      return caf::none;
    }
  }
  __builtin_unreachable();
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
  return caf::visit(
    [](auto& rng) {
      return rng.done();
    },
    range_);
}

bitmap_bit_range bit_range(const bitmap& bm) {
  return bitmap_bit_range{bm};
}

} // namespace vast
