//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"

#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/params.h>

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>

namespace tenzir::openssl {

enum class algorithm {
  md5,
  sha1,
  sha224,
  sha256,
  sha384,
  sha512,
  sha3_224,
  sha3_256,
  sha3_384,
  sha3_512,
};

namespace detail {

template <algorithm Algorithm>
struct algorithm_traits;

template <>
struct algorithm_traits<algorithm::md5> {
  static constexpr size_t digest_size = 128 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_md5();
  }
};

template <>
struct algorithm_traits<algorithm::sha1> {
  static constexpr size_t digest_size = 160 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_sha1();
  }
};

template <>
struct algorithm_traits<algorithm::sha224> {
  static constexpr size_t digest_size = 224 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_sha224();
  }
};

template <>
struct algorithm_traits<algorithm::sha256> {
  static constexpr size_t digest_size = 256 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_sha256();
  }
};

template <>
struct algorithm_traits<algorithm::sha384> {
  static constexpr size_t digest_size = 384 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_sha384();
  }
};

template <>
struct algorithm_traits<algorithm::sha512> {
  static constexpr size_t digest_size = 512 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_sha512();
  }
};

template <>
struct algorithm_traits<algorithm::sha3_224> {
  static constexpr size_t digest_size = 224 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_sha3_224();
  }
};

template <>
struct algorithm_traits<algorithm::sha3_256> {
  static constexpr size_t digest_size = 256 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_sha3_256();
  }
};

template <>
struct algorithm_traits<algorithm::sha3_384> {
  static constexpr size_t digest_size = 384 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_sha3_384();
  }
};

template <>
struct algorithm_traits<algorithm::sha3_512> {
  static constexpr size_t digest_size = 512 / 8;
  static const EVP_MD* evp() noexcept {
    return EVP_sha3_512();
  }
};

template <typename T, auto f>
  requires requires(T* ptr) { f(ptr); }
struct deleter {
  static auto operator()(T* ptr) noexcept -> void {
    f(ptr);
  };
};

template <typename T, auto f>
using smart_pointer = std::unique_ptr<T, deleter<T, f>>;

} // namespace detail

template <algorithm Algorithm>
class hash {
public:
  static constexpr auto digest_size
    = detail::algorithm_traits<Algorithm>::digest_size;
  using result_type = std::array<std::byte, digest_size>;

  static constexpr std::endian endian = std::endian::native;

  hash() noexcept : ctx_{EVP_MD_CTX_new()} {
    TENZIR_ASSERT_ALWAYS(ctx_ != nullptr);
    const auto* md = detail::algorithm_traits<Algorithm>::evp();
    const auto init_ok = EVP_DigestInit_ex(ctx_.get(), md, nullptr);
    TENZIR_ASSERT_ALWAYS(init_ok == 1);
  }

  void add(std::span<const std::byte> bytes) noexcept {
    if (bytes.empty()) {
      return;
    }
    TENZIR_ASSERT_ALWAYS(! finished_);
    const auto* ptr = reinterpret_cast<const unsigned char*>(bytes.data());
    const auto update_ok = EVP_DigestUpdate(ctx_.get(), ptr, bytes.size());
    TENZIR_ASSERT_ALWAYS(update_ok == 1);
  }

  auto finish() noexcept -> result_type {
    if (! finished_) {
      auto* out = reinterpret_cast<unsigned char*>(digest_.data());
      const auto final_ok = EVP_DigestFinal_ex(ctx_.get(), out, nullptr);
      TENZIR_ASSERT_ALWAYS(final_ok == 1);
      finished_ = true;
      ctx_.reset();
    }
    return digest_;
  }

  hash(const hash&) = delete;
  auto operator=(const hash&) -> hash& = delete;
  hash(hash&&) noexcept = default;
  auto operator=(hash&&) noexcept -> hash& = default;
  ~hash() = default;

private:
  std::array<std::byte, digest_size> digest_{};
  bool finished_ = false;
  detail::smart_pointer<EVP_MD_CTX, EVP_MD_CTX_free> ctx_{};
};

template <algorithm Algorithm>
class hmac {
public:
  static constexpr auto digest_size
    = detail::algorithm_traits<Algorithm>::digest_size;
  using result_type = std::array<std::byte, digest_size>;

  static constexpr std::endian endian = std::endian::native;

  template <size_t Extent = std::dynamic_extent>
  explicit hmac(std::span<const std::byte, Extent> key) noexcept
    : mac_{EVP_MAC_fetch(nullptr, "HMAC", nullptr)},
      ctx_{EVP_MAC_CTX_new(mac_.get())} {
    TENZIR_ASSERT_ALWAYS(mac_ != nullptr);
    TENZIR_ASSERT_ALWAYS(ctx_ != nullptr);
    const auto* md = detail::algorithm_traits<Algorithm>::evp();
    const auto* md_name = EVP_MD_get0_name(md);
    TENZIR_ASSERT_ALWAYS(md_name != nullptr);
    OSSL_PARAM params[2];
    params[0] = OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST,
                                                 const_cast<char*>(md_name), 0);
    params[1] = OSSL_PARAM_construct_end();
    const auto params_ok = EVP_MAC_CTX_set_params(ctx_.get(), params);
    TENZIR_ASSERT_ALWAYS(params_ok == 1);
    const auto* key_ptr = reinterpret_cast<const unsigned char*>(key.data());
    const auto init_ok = EVP_MAC_init(ctx_.get(), key_ptr, key.size(), nullptr);
    TENZIR_ASSERT_ALWAYS(init_ok == 1);
  }

  void add(std::span<const std::byte> bytes) noexcept {
    if (bytes.empty()) {
      return;
    }
    TENZIR_ASSERT_ALWAYS(! finished_);
    const auto* ptr = reinterpret_cast<const unsigned char*>(bytes.data());
    const auto update_ok = EVP_MAC_update(ctx_.get(), ptr, bytes.size());
    TENZIR_ASSERT_ALWAYS(update_ok == 1);
  }

  auto finish() noexcept -> result_type {
    if (! finished_) {
      auto* out = reinterpret_cast<unsigned char*>(digest_.data());
      size_t len = 0;
      const auto final_ok = EVP_MAC_final(ctx_.get(), out, &len, digest_size);
      TENZIR_ASSERT_ALWAYS(final_ok == 1);
      TENZIR_ASSERT_ALWAYS(len == digest_size);
      finished_ = true;
      ctx_.reset();
    }
    return digest_;
  }

  hmac(const hmac&) = delete;
  auto operator=(const hmac&) -> hmac& = delete;
  hmac(hmac&&) noexcept = default;
  auto operator=(hmac&&) noexcept -> hmac& = default;
  ~hmac() = default;

private:
  std::array<std::byte, digest_size> digest_{};
  bool finished_ = false;
  detail::smart_pointer<EVP_MAC, EVP_MAC_free> mac_;
  detail::smart_pointer<EVP_MAC_CTX, EVP_MAC_CTX_free> ctx_{};
};

} // namespace tenzir::openssl
