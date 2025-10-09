//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/uuid.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/uuid.hpp"

#include <cstddef>
#include <cstring>
#include <random>
#include <span>

namespace tenzir {
namespace {

class random_generator {
  using number_type = unsigned long;
  using distribution = std::uniform_int_distribution<number_type>;

public:
  random_generator()
    : unif_{std::numeric_limits<number_type>::min(),
            std::numeric_limits<number_type>::max()} {
  }

  uuid operator()() {
    uuid result;
    auto r = unif_(rd_);
    int i = 0;
    for (auto& x : result) {
      if (i == sizeof(number_type)) {
        r = unif_(rd_);
        i = 0;
      }
      x = detail::narrow_cast<std::byte>((r >> (i * 8)) & 0xff);
      ++i;
    }
    // Set variant to 0b10xxxxxx.
    result[8] &= std::byte{0xbf};
    result[8] |= std::byte{0x80};
    // Set version to 0b0100xxxx.
    result[6] &= std::byte{0x4f}; // 0b01001111
    result[6] |= std::byte{0x40}; // 0b01000000
    return result;
  }

private:
  std::random_device rd_;
  distribution unif_;
};

} // namespace

uuid uuid::random() {
  return random_generator{}();
}

uuid uuid::null() {
  uuid u;
  u.id_.fill(std::byte{0});
  return u;
}

uuid uuid::from_flatbuffer(const fbs::UUID& id) {
  TENZIR_ASSERT(id.data()->size() == uuid::num_bytes);
  auto const* data = id.data();
  return uuid{std::span<const std::byte, uuid::num_bytes>(
    reinterpret_cast<const std::byte*>(data->data()), data->size())};
}

uuid::uuid(std::span<const std::byte, num_bytes> bytes) {
  std::memcpy(id_.data(), bytes.data(), bytes.size());
}

uuid::reference uuid::operator[](size_t i) {
  return id_[i];
}

uuid::const_reference uuid::operator[](size_t i) const {
  return id_[i];
}

std::pair<uint64_t, uint64_t> uuid::as_u64() const {
  auto lh = *reinterpret_cast<const uint64_t*>(&id_[0]);
  auto rh = *reinterpret_cast<const uint64_t*>(&id_[8]);
  return std::make_pair(lh, rh);
}

uuid::iterator uuid::begin() {
  return id_.begin();
}

uuid::iterator uuid::end() {
  return id_.end();
}

uuid::const_iterator uuid::begin() const {
  return id_.begin();
}

uuid::const_iterator uuid::end() const {
  return id_.end();
}

uuid::size_type uuid::size() const {
  return num_bytes;
}

std::strong_ordering operator<=>(const uuid& lhs, const uuid& rhs) noexcept {
  if (&lhs == &rhs) {
    return std::strong_ordering::equal;
  }
  const auto result = std::memcmp(lhs.begin(), rhs.begin(), uuid::num_bytes);
  return result == 0  ? std::strong_ordering::equivalent
         : result < 0 ? std::strong_ordering::less
                      : std::strong_ordering::greater;
}

bool operator==(const uuid& lhs, const uuid& rhs) noexcept {
  return lhs.id_ == rhs.id_;
}

caf::expected<flatbuffers::Offset<fbs::LegacyUUID>>
pack(flatbuffers::FlatBufferBuilder& builder, const uuid& x) {
  auto data = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(&*x.begin()), x.size());
  fbs::LegacyUUIDBuilder uuid_builder{builder};
  uuid_builder.add_data(data);
  return uuid_builder.Finish();
}

caf::error unpack(const fbs::LegacyUUID& x, uuid& y) {
  if (x.data()->size() != uuid::num_bytes) {
    return caf::make_error(ec::format_error, "wrong uuid format");
  }
  std::span<const std::byte, uuid::num_bytes> bytes{
    reinterpret_cast<const std::byte*>(x.data()->data()), x.data()->size()};
  y = uuid{bytes};
  return caf::none;
}

auto inspect(caf::detail::stringification_inspector& f, uuid& x) {
  return f.apply(fmt::to_string(x));
}
} // namespace tenzir
