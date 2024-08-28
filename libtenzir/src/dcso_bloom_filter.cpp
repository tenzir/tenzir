//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/dcso_bloom_filter.hpp"

#include "tenzir/detail/byteswap.hpp"
#include "tenzir/detail/inspection_common.hpp"

#include <bit>
#include <cmath>
#include <cstddef>

namespace tenzir {

class dcso_bloom_filter::serializer {
public:
  using result_type = bool;

  static constexpr bool is_loading = false;

  explicit serializer(std::vector<std::byte>& bytes) : bytes_{bytes} {
  }

  template <class T>
  auto object(const T&) {
    return tenzir::detail::inspection_object(*this);
  }

  template <class T>
  auto field(std::string_view, T& value) {
    return tenzir::detail::inspection_field{value};
  }

  auto operator()(const dcso_bloom_filter& x) -> result_type {
    // Allocate memory once.
    bytes_.reserve(6 * 8 + x.bloom_filter_.size() * 8 + x.data_.size());
    // Write version.
    write(1);
    // Write parameters.
    write(*x.params_.n);
    write(std::bit_cast<uint64_t>(*x.params_.p));
    write(*x.params_.k);
    write(*x.params_.m);
    write(x.num_elements_);
    // Write Bloom filter.
    if (!inspect(*this, const_cast<bloom_filter_type&>(x.bloom_filter_))) {
      return false;
    }
    // Write auxiliary data.
    bytes_.insert(bytes_.end(), x.data_.begin(), x.data_.end());
    return true;
  }

  auto operator()(const bloom_filter_type::hasher_type&) -> result_type {
    // Nothing to do here since the hasher's state only contains the
    // parameter k, which we have already stored earlier.
    return true;
  }

  auto operator()(const bitvector<uint64_t>& xs) -> result_type {
    for (auto block : xs.blocks()) {
      write(block);
    }
    return true;
  }

  template <class T, class... Ts>
  auto operator()(const Ts&... xs) noexcept -> result_type {
    return ((*this)(xs) && ...);
  }

  auto apply(auto& x) -> result_type {
    return (*this)(x);
  }

  void write(uint64_t value) {
    auto le = detail::swap<std::endian::native, std::endian::little>(value);
    auto bytes = std::bit_cast<std::array<std::byte, 8>>(le);
    bytes_.insert(bytes_.end(), bytes.begin(), bytes.end());
  }

private:
  std::vector<std::byte>& bytes_;
};

class dcso_bloom_filter::deserializer {
public:
  using result_type = bool;

  static constexpr bool is_loading = true;

  explicit deserializer(std::span<const std::byte> bytes) : bytes_{bytes} {
    TENZIR_ASSERT(bytes_.size() >= min_buffer_size);
  }

  template <class T>
  auto object(const T&) {
    return detail::inspection_object(*this);
  }

  template <class T>
  auto field(std::string_view, T& value) {
    return detail::inspection_field{value};
  }

  auto operator()(dcso_bloom_filter& x) -> result_type {
    // Read header (version + parameters).
    auto header = bytes_.subspan<0, header_bytes>();
    // Check version.
    auto version_bytes = header.subspan<0, 8>();
    if (auto version = read(version_bytes); version != 1) {
      last_error_ = caf::make_error(ec::parse_error, "invalid version");
      return false;
    }
    x.params_.n = read(header.subspan<8, 8>());
    x.params_.p = std::bit_cast<double>(read(header.subspan<16, 8>()));
    x.params_.k = read(header.subspan<24, 8>());
    if (x.params_.k >= std::numeric_limits<int>::max()) {
      last_error_
        = caf::make_error(ec::parse_error, "k must not exceed max int");
      return false;
    }
    x.params_.m = read(header.subspan<32, 8>());
    x.num_elements_ = read(header.subspan<40, 8>());
    // Make some values available in this visitor for other overloads.
    k_ = *x.params_.k;
    m_ = *x.params_.m;
    auto bloom_filter_blocks = (*x.params_.m + 63) / 64;
    auto bloom_filter_bytes = bloom_filter_blocks * 8;
    // Read Bloom filter after all 6 uint64_t values.
    auto remaining_bytes = bytes_.size() - header_bytes;
    if (remaining_bytes < bloom_filter_bytes) {
      last_error_ = caf::make_error(
        ec::parse_error,
        fmt::format("filter too small: expected {} bytes but buffer "
                    "has only {} bytes remaining",
                    bloom_filter_bytes, remaining_bytes));
      return false;
    }
    if (!inspect(*this, x.bloom_filter_)) {
      return false;
    }
    // Interpret remaining bytes as auxiliary data.
    if (bloom_filter_bytes < remaining_bytes) {
      auto data = bytes_.subspan(header_bytes + bloom_filter_bytes);
      TENZIR_ASSERT(data.size() > 0);
      x.data_.resize(data.size());
      std::copy(data.begin(), data.end(), x.data_.begin());
    }
    return true;
  }

  auto operator()(bloom_filter_type::hasher_type& x) const -> result_type {
    // Take k from the Bloom filter parameters.
    x = dcso_bloom_filter::bloom_filter_type::hasher_type{k_};
    return true;
  }

  auto operator()(bitvector<uint64_t>& xs) -> result_type {
    auto bloom_filter_blocks = (m_ + 63) / 64;
    auto blocks = bytes_.subspan(header_bytes, bloom_filter_blocks * 8);
    xs.clear();
    xs.reserve(blocks.size());
    for (size_t i = 0; i < bloom_filter_blocks; ++i) {
      auto block = blocks.subspan(i * 8).subspan<0, 8>();
      xs.append_block(read(block));
    }
    if (m_ % 64 != 0) {
      xs.resize(m_);
    }
    return true;
  }

  template <class... Ts>
    requires(sizeof...(Ts) != 1)
  auto operator()(Ts&... xs) noexcept -> result_type {
    return ((*this)(xs) && ...);
  }

  auto apply(auto& x) -> result_type {
    return (*this)(x);
  }

  auto last_error() && -> caf::error {
    return std::move(last_error_);
  }

  static auto read(std::span<const std::byte, 8> bytes) -> uint64_t {
    auto result = uint64_t{0};
    std::memcpy(&result, bytes.data(), sizeof(result));
    return detail::swap<std::endian::little, std::endian::native>(result);
  }

private:
  std::span<const std::byte> bytes_;
  uint64_t k_{0u};
  uint64_t m_{0u};
  caf::error last_error_;
};

auto dcso_bloom_filter::m(uint64_t n, double p) -> uint64_t {
  TENZIR_ASSERT(p > 0 && p < 1);
  TENZIR_ASSERT(n > 0);
  auto result = double(n) * std::log(p) / std::pow(std::log(2.0), 2.0);
  return std::abs(std::ceil(result));
}

auto dcso_bloom_filter::k(uint64_t n, double p) -> uint64_t {
  TENZIR_ASSERT(n > 0);
  return std::ceil(std::log(2) * m(n, p) / static_cast<double>(n));
}

dcso_bloom_filter::dcso_bloom_filter() : bloom_filter_{0, hasher_type{1}} {
  params_.m = 0;
  params_.n = 0;
  params_.k = 1;
  params_.p = 1.0;
}

dcso_bloom_filter::dcso_bloom_filter(uint64_t n, double p)
  : bloom_filter_{m(n, p), hasher_type{k(n, p)}} {
  params_.m = m(n, p);
  params_.n = n;
  params_.k = k(n, p);
  params_.p = p;
}

auto dcso_bloom_filter::parameters() const -> const bloom_filter_parameters& {
  return params_;
}

auto dcso_bloom_filter::num_elements() const -> uint64_t {
  return num_elements_;
}

/// Access the attached data.
auto dcso_bloom_filter::data() const -> const std::vector<std::byte>& {
  return data_;
}

/// Access the attached data.
auto dcso_bloom_filter::data() -> std::vector<std::byte>& {
  return data_;
}

auto convert(std::span<const std::byte> xs, dcso_bloom_filter& x)
  -> caf::error {
  if (xs.size() < dcso_bloom_filter::min_buffer_size) {
    return caf::make_error(ec::parse_error, "bloom filter buffer too small");
  }
  dcso_bloom_filter::deserializer source{xs};
  if (!source(x)) {
    if (auto err = std::move(source).last_error()) {
      return err;
    }
    return caf::make_error(ec::parse_error, "dcso_bloom_filter deserialization "
                                            "failed");
  }
  return {};
}

auto convert(const dcso_bloom_filter& x, std::vector<std::byte>& xs)
  -> caf::error {
  dcso_bloom_filter::serializer sink{xs};
  if (sink(x)) {
    return {};
  }
  return caf::make_error(ec::convert_error, "dcso_bloom_filter serialization "
                                            "failed");
}

auto operator==(const dcso_bloom_filter& x, const dcso_bloom_filter& y)
  -> bool {
  return x.bloom_filter_ == y.bloom_filter_ && x.params_ == y.params_
         && x.num_elements_ == y.num_elements_ && x.data_ == y.data_;
}

} // namespace tenzir
