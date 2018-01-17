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

#include <cstring>

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/compressedbuf.hpp"
#include "vast/detail/varbyte.hpp"

namespace vast {
namespace detail {

compressedbuf::compressedbuf(std::streambuf& sb, compression method,
                            size_t block_size)
  : streambuf_{sb},
    method_{method},
    block_size_{block_size} {
  VAST_ASSERT(block_size > 0);
  compressed_.resize(block_size_);
  uncompressed_.resize(block_size_);
  setp(uncompressed_.data(), uncompressed_.data() + uncompressed_.size());
}

int compressedbuf::sync() {
  if (pbase() == nullptr)
    return -1;
  if (uncompressed_.empty())
    return 0;
  size_t uncompressed_size = pptr() - pbase();
  uncompressed_.resize(uncompressed_size);
  compress();
  auto total = 0;
  // Write header.
  char size[16];
  auto n = varbyte::encode(uncompressed_size, size);
  size_t put = streambuf_.sputn(size, n);
  if (put != n)
    return -1;
  total += put;
  n = varbyte::encode(compressed_.size(), size);
  put = streambuf_.sputn(size, n);
  if (put != n)
    return -1;
  total += put;
  // Write data block.
  put = streambuf_.sputn(compressed_.data(), compressed_.size());
  if (put != compressed_.size())
    return -1;
  total += put;
  // Reset put area.
  setp(uncompressed_.data(), uncompressed_.data() + uncompressed_.size());
  return total;
}

compressedbuf::int_type compressedbuf::overflow(int_type c) {
  // Handle given character.
  if (traits_type::eq_int_type(c, traits_type::eof()))
    return traits_type::not_eof(c);
  auto bytes = sync();
  if (bytes < 0)
    return traits_type::eof(); // indicates failure
  uncompressed_[0] = traits_type::to_char_type(c);
  pbump(1);
  return c;
}

std::streamsize compressedbuf::xsputn(char_type const* s, std::streamsize n) {
  auto put = std::streamsize{0};
  while (put < n) {
    while (epptr() == pptr()) {
      auto c = overflow(traits_type::to_int_type(s[put]));
      if (traits_type::eq_int_type(c, traits_type::eof()) || ++put == n)
        return put;
    }
    std::streamsize available = epptr() - pptr();
    auto bytes = std::min(n - put, available);
    std::memcpy(pptr(), s + put, bytes);
    put += bytes;
    pbump(bytes);
  };
  return put;
}

compressedbuf::int_type compressedbuf::underflow() {
  VAST_ASSERT(gptr() == nullptr || gptr() >= egptr());
  // Read header.
  static auto read_varbyte = [](std::streambuf& source, char* sink) -> bool {
    auto p = sink;
    int_type i;
    char_type c;
    do {
      i = source.sbumpc();
      if (traits_type::eq_int_type(i, traits_type::eof()))
         return false;
      c = traits_type::to_char_type(i);
      *p++ = c;
    } while (c & 0x80);
    return true;
  };
  char size[16];
  uint32_t uncompressed_size, compressed_size;
  if (!read_varbyte(streambuf_, size))
    return traits_type::eof();
  varbyte::decode(uncompressed_size, size);
  if (!read_varbyte(streambuf_, size))
    return traits_type::eof();
  varbyte::decode(compressed_size, size);
  // Adjust buffers.
  uncompressed_.resize(uncompressed_size);
  compressed_.resize(compressed_size);
  // Retrieve compressed data block.
  size_t got = streambuf_.sgetn(compressed_.data(), compressed_size);
  if (got != compressed_size)
    return traits_type::eof();
  // Uncompress data.
  uncompress();
  // Reset get area.
  setg(uncompressed_.data(),
       uncompressed_.data(),
       uncompressed_.data() + uncompressed_.size());
  return traits_type::to_int_type(*gptr());
}

std::streamsize compressedbuf::xsgetn(char_type* s, std::streamsize n) {
  auto got = std::streamsize{0};
  while (got < n) {
    while (egptr() == gptr()) {
      auto c = underflow();
      if (traits_type::eq_int_type(c, traits_type::eof()))
        return got;
      s[got] = traits_type::to_char_type(c);
      gbump(1);
      if (++got == n)
        return got;
    }
    std::streamsize available = egptr() - gptr();
    auto bytes = std::min(n - got, available);
    std::memcpy(s + got, gptr(), bytes);
    got += bytes;
    gbump(bytes);
  };
  return got;
}

void compressedbuf::compress() {
  size_t n = 0;
  switch (method_) {
    case compression::null:
      compressed_.resize(uncompressed_.size());
      std::memcpy(compressed_.data(), uncompressed_.data(),
                  uncompressed_.size());
      n = uncompressed_.size();
      break;
    case compression::lz4: {
      compressed_.resize(lz4::compress_bound(uncompressed_.size()));
      n = lz4::compress(uncompressed_.data(), uncompressed_.size(),
                        compressed_.data(), compressed_.size());
      break;
    }
#ifdef VAST_HAVE_SNAPPY
    case compression::snappy: {
      compressed_.resize(snappy::compress_bound(uncompressed_.size()));
      n = snappy::compress(pbase(), uncompressed_.size(), compressed_.data());
      break;
    }
#endif // VAST_HAVE_SNAPPY
  }
  compressed_.resize(n);
  uncompressed_.resize(block_size_);
}

void compressedbuf::uncompress() {
  size_t n = 0;
  switch (method_) {
    case compression::null: {
      std::memcpy(uncompressed_.data(), compressed_.data(), compressed_.size());
      n = compressed_.size();
      break;
    }
    case compression::lz4: {
      n = lz4::uncompress(compressed_.data(), compressed_.size(),
                          uncompressed_.data(), uncompressed_.size());
      break;
    }
#ifdef VAST_HAVE_SNAPPY
    case compression::snappy: {
      auto success = snappy::uncompress(compressed_.data(), compressed_.size(),
                                        uncompressed_.data());
      VAST_ASSERT(success);
      n = snappy::uncompress_bound(compressed_.data(), compressed_.size());
      break;
    }
#endif // VAST_HAVE_SNAPPY
  }
  VAST_ASSERT(n > 0);
  uncompressed_.resize(n);
  compressed_.resize(block_size_);
}

} // namespace detail
} // namespace vast
