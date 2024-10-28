//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Most of the actual implementation in this file comes from a 3rd party and
// has been adapted to fit into the Tenzir code base. Details about the original
// file:
//
// - Repository: https://github.com/kerukuro/digestpp
// - Commit:     6460289803d9c85ae755b324994b759e624c5f9a
// - Path:       algorithm/detail/sha2_provider.hpp
// - Author:     kerukuro
// - License:    The Unlicense

#pragma once

#include "tenzir/as_bytes.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/byteswap.hpp"
#include "tenzir/hash/utils.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <span>

namespace tenzir {

/// The [SHA-2](https://en.wikipedia.org/wiki/SHA-2) hash algorithm family.
/// This implementation comes from https://github.com/kerukuro/digestpp.
template <class T, size_t O = 0>
class sha2 {
  static_assert(std::is_same_v<T, uint32_t> or std::is_same_v<T, uint64_t>,
                "T must be uint32_t or uint64_t");

public:
  constexpr static size_t N = sizeof(T) == 8 ? 512 : 256;
  using result_type = std::vector<std::byte>;

  static constexpr std::endian endian = std::endian::native;

  sha2() noexcept
    requires(O != 0 and (sizeof(T) == 4 or O == 384))
    : hash_size_{O} {
    init();
  }

  sha2(size_t hash_size = N) noexcept
    requires(O == 0 and sizeof(T) == 8)
    : hash_size_{hash_size} {
    // TODO: validate hash size
    init();
  }

  void add(std::span<const std::byte> bytes) noexcept {
    auto f = [this](const unsigned char* data, size_t len) {
      transform(data, len);
    };
    const auto* ptr = reinterpret_cast<const unsigned char*>(bytes.data());
    detail::absorb_bytes(ptr, bytes.size(), N / 4, N / 4, m_.data(), pos_,
                         total_, f);
  }

  auto finish() noexcept -> result_type {
    finalize();
    auto result = result_type{};
    result.resize(hash_size_ / 8);
    std::memcpy(result.data(), H_.data(), result.size());
    return result;
  }

  friend auto inspect(auto& f, sha2& x) {
    return f.object(x).fields(f.field("H", x.H_), f.field("m", x.m_),
                              f.field("pos", x.pos_),
                              f.field("total", x.total_));
  }

private:
  static auto Ch(T x, T y, T z) -> T {
    return (x & y) ^ (~x & z);
  }

  static auto Maj(T x, T y, T z) -> T {
    return (x & y) ^ (x & z) ^ (y & z);
  }

  static auto sum0(uint64_t x) {
    return detail::rotate_right(x, 28) ^ detail::rotate_right(x, 34)
           ^ detail::rotate_right(x, 39);
  }

  static auto sum1(uint64_t x) {
    return detail::rotate_right(x, 14) ^ detail::rotate_right(x, 18)
           ^ detail::rotate_right(x, 41);
  }

  static auto sigma0(uint64_t x) {
    return detail::rotate_right(x, 1) ^ detail::rotate_right(x, 8) ^ (x >> 7);
  }

  static auto sigma1(uint64_t x) {
    return detail::rotate_right(x, 19) ^ detail::rotate_right(x, 61) ^ (x >> 6);
  }

  static auto sum0(uint32_t x) {
    return detail::rotate_right(x, 2) ^ detail::rotate_right(x, 13)
           ^ detail::rotate_right(x, 22);
  }

  static auto sum1(uint32_t x) {
    return detail::rotate_right(x, 6) ^ detail::rotate_right(x, 11)
           ^ detail::rotate_right(x, 25);
  }

  static auto sigma0(uint32_t x) {
    return detail::rotate_right(x, 7) ^ detail::rotate_right(x, 18) ^ (x >> 3);
  }

  static auto sigma1(uint32_t x) {
    return detail::rotate_right(x, 17) ^ detail::rotate_right(x, 19)
           ^ (x >> 10);
  }

  auto init() noexcept -> void {
    if constexpr (std::is_same_v<T, uint32_t> && O == 224) {
      H_[0] = 0xc1059ed8;
      H_[1] = 0x367cd507;
      H_[2] = 0x3070dd17;
      H_[3] = 0xf70e5939;
      H_[4] = 0xffc00b31;
      H_[5] = 0x68581511;
      H_[6] = 0x64f98fa7;
      H_[7] = 0xbefa4fa4;
    } else if constexpr (std::is_same_v<T, uint32_t> && O == 256) {
      H_[0] = 0x6a09e667;
      H_[1] = 0xbb67ae85;
      H_[2] = 0x3c6ef372;
      H_[3] = 0xa54ff53a;
      H_[4] = 0x510e527f;
      H_[5] = 0x9b05688c;
      H_[6] = 0x1f83d9ab;
      H_[7] = 0x5be0cd19;
    } else if (std::is_same_v<T, uint64_t>) {
      switch (hash_size_) {
        case 224:
          H_[0] = 0x8C3D37C819544DA2ull;
          H_[1] = 0x73E1996689DCD4D6ull;
          H_[2] = 0x1DFAB7AE32FF9C82ull;
          H_[3] = 0x679DD514582F9FCFull;
          H_[4] = 0x0F6D2B697BD44DA8ull;
          H_[5] = 0x77E36F7304C48942ull;
          H_[6] = 0x3F9D85A86A1D36C8ull;
          H_[7] = 0x1112E6AD91D692A1ull;
          return;
        case 256:
          H_[0] = 0x22312194FC2BF72Cull;
          H_[1] = 0x9F555FA3C84C64C2ull;
          H_[2] = 0x2393B86B6F53B151ull;
          H_[3] = 0x963877195940EABDull;
          H_[4] = 0x96283EE2A88EFFE3ull;
          H_[5] = 0xBE5E1E2553863992ull;
          H_[6] = 0x2B0199FC2C85B8AAull;
          H_[7] = 0x0EB72DDC81C52CA2ull;
          return;
        case 384:
          H_[0] = 0xcbbb9d5dc1059ed8ull;
          H_[1] = 0x629a292a367cd507ull;
          H_[2] = 0x9159015a3070dd17ull;
          H_[3] = 0x152fecd8f70e5939ull;
          H_[4] = 0x67332667ffc00b31ull;
          H_[5] = 0x8eb44a8768581511ull;
          H_[6] = 0xdb0c2e0d64f98fa7ull;
          H_[7] = 0x47b5481dbefa4fa4ull;
          return;
        default:
          H_[0] = 0x6a09e667f3bcc908ull;
          H_[1] = 0xbb67ae8584caa73bull;
          H_[2] = 0x3c6ef372fe94f82bull;
          H_[3] = 0xa54ff53a5f1d36f1ull;
          H_[4] = 0x510e527fade682d1ull;
          H_[5] = 0x9b05688c2b3e6c1full;
          H_[6] = 0x1f83d9abfb41bd6bull;
          H_[7] = 0x5be0cd19137e2179ull;
      }
      if (hash_size_ == 512) {
        return;
      }
      // Calculate initial values for SHA-512/t with a different output size.
      for (int i = 0; i < 8; i++) {
        H_[i] ^= 0xa5a5a5a5a5a5a5a5ull;
      }
      std::string tmp = "SHA-512/" + std::to_string(hash_size_);
      add(as_bytes(tmp));
      finalize();
      for (int i = 0; i < 8; i++) {
        H_[i] = detail::byteswap(H_[i]);
      }
      pos_ = 0;
      total_ = 0;
    }
  }

  void finalize() {
    total_ += pos_ * 8;
    m_[pos_++] = 0x80;
    if (pos_ > N / 4 - sizeof(T) * 2) {
      if (pos_ != N / 4) {
        memset(&m_[pos_], 0, N / 4 - pos_);
      }
      transform(m_.data(), 1);
      pos_ = 0;
    }
    memset(&m_[pos_], 0, N / 4 - pos_);
    uint64_t mlen = detail::byteswap(total_);
    memcpy(&m_[N / 4 - 8], &mlen, 64 / 8);
    transform(m_.data(), 1);
    for (int i = 0; i < 8; i++) {
      H_[i] = detail::byteswap(H_[i]);
    }
  }

  void transform(const unsigned char* data, size_t num_blks) {
    for (size_t blk = 0; blk < num_blks; blk++) {
      T m_[16];
      for (int i = 0; i < 16; i++) {
        m_[i]
          = detail::byteswap(reinterpret_cast<const T*>(data)[blk * 16 + i]);
      }
      const int rounds = N == 512 ? 80 : 64;
      T W[rounds];
      for (int t = 0; t <= 15; t++) {
        W[t] = m_[t];
      }
      for (int t = 16; t < rounds; t++) {
        W[t] = sigma1(W[t - 2]) + W[t - 7] + sigma0(W[t - 15]) + W[t - 16];
      }
      T a = H_[0];
      T b = H_[1];
      T c = H_[2];
      T d = H_[3];
      T e = H_[4];
      T f = H_[5];
      T g = H_[6];
      T h = H_[7];
      for (int t = 0; t < rounds; t++) {
        T T1 = h + sum1(e) + Ch(e, f, g) + getK(t) + W[t];
        T T2 = sum0(a) + Maj(a, b, c);
        h = g;
        g = f;
        f = e;
        e = d + T1;
        d = c;
        c = b;
        b = a;
        a = T1 + T2;
      }
      H_[0] += a;
      H_[1] += b;
      H_[2] += c;
      H_[3] += d;
      H_[4] += e;
      H_[5] += f;
      H_[6] += g;
      H_[7] += h;
    }
  }

  auto getK(int t) {
    if constexpr (std::is_same_v<T, uint32_t>) {
      static constexpr auto K = std::array<T, 64>{
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
        0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
        0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
        0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
        0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
        0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
        0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};
      return K[t];
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      static constexpr auto K = std::array<T, 80>{
        0x428a2f98d728ae22ull, 0x7137449123ef65cdull, 0xb5c0fbcfec4d3b2full,
        0xe9b5dba58189dbbcull, 0x3956c25bf348b538ull, 0x59f111f1b605d019ull,
        0x923f82a4af194f9bull, 0xab1c5ed5da6d8118ull, 0xd807aa98a3030242ull,
        0x12835b0145706fbeull, 0x243185be4ee4b28cull, 0x550c7dc3d5ffb4e2ull,
        0x72be5d74f27b896full, 0x80deb1fe3b1696b1ull, 0x9bdc06a725c71235ull,
        0xc19bf174cf692694ull, 0xe49b69c19ef14ad2ull, 0xefbe4786384f25e3ull,
        0x0fc19dc68b8cd5b5ull, 0x240ca1cc77ac9c65ull, 0x2de92c6f592b0275ull,
        0x4a7484aa6ea6e483ull, 0x5cb0a9dcbd41fbd4ull, 0x76f988da831153b5ull,
        0x983e5152ee66dfabull, 0xa831c66d2db43210ull, 0xb00327c898fb213full,
        0xbf597fc7beef0ee4ull, 0xc6e00bf33da88fc2ull, 0xd5a79147930aa725ull,
        0x06ca6351e003826full, 0x142929670a0e6e70ull, 0x27b70a8546d22ffcull,
        0x2e1b21385c26c926ull, 0x4d2c6dfc5ac42aedull, 0x53380d139d95b3dfull,
        0x650a73548baf63deull, 0x766a0abb3c77b2a8ull, 0x81c2c92e47edaee6ull,
        0x92722c851482353bull, 0xa2bfe8a14cf10364ull, 0xa81a664bbc423001ull,
        0xc24b8b70d0f89791ull, 0xc76c51a30654be30ull, 0xd192e819d6ef5218ull,
        0xd69906245565a910ull, 0xf40e35855771202aull, 0x106aa07032bbd1b8ull,
        0x19a4c116b8d2d0c8ull, 0x1e376c085141ab53ull, 0x2748774cdf8eeb99ull,
        0x34b0bcb5e19b48a8ull, 0x391c0cb3c5c95a63ull, 0x4ed8aa4ae3418acbull,
        0x5b9cca4f7763e373ull, 0x682e6ff3d6b2b8a3ull, 0x748f82ee5defb2fcull,
        0x78a5636f43172f60ull, 0x84c87814a1f0ab72ull, 0x8cc702081a6439ecull,
        0x90befffa23631e28ull, 0xa4506cebde82bde9ull, 0xbef9a3f7b2c67915ull,
        0xc67178f2e372532bull, 0xca273eceea26619cull, 0xd186b8c721c0c207ull,
        0xeada7dd6cde0eb1eull, 0xf57d4f7fee6ed178ull, 0x06f067aa72176fbaull,
        0x0a637dc5a2c898a6ull, 0x113f9804bef90daeull, 0x1b710b35131c471bull,
        0x28db77f523047d84ull, 0x32caab7b40c72493ull, 0x3c9ebe0a15c9bebcull,
        0x431d67c49c100d4cull, 0x4cc5d4becb3e42b6ull, 0x597f299cfc657e2aull,
        0x5fcb6fab3ad6faecull, 0x6c44198c4a475817ull};
      return K[t];
    } else {
      TENZIR_UNREACHABLE();
    }
  }

  std::array<T, 8> H_;
  std::array<unsigned char, N / 4> m_;
  size_t pos_ = 0;
  uint64_t total_ = 0;
  size_t hash_size_;
};

using sha224 = sha2<uint32_t, 224>;
using sha256 = sha2<uint32_t, 256>;
using sha384 = sha2<uint64_t, 384>;
using sha512 = sha2<uint64_t>;

} // namespace tenzir
