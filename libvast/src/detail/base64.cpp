// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

// Most of the actual implementation in this file comes from a 3rd party and
// has been adapted to fit into the VAST code base. Details about the original
// file:
//
// - Repository: https://github.com/boostorg/beast
// - Commit:     8848ced9ab1526053393b82a65449e651ae87cd2
// - Path:       beast/include/boost/beast/core/detail/base64.ipp
// - Author:     Vinnie Falco <vinnie.falco@gmail.com>
// - License:    Boost Software License, Version 1.0

#include "vast/detail/base64.hpp"

namespace vast::detail::base64 {

size_t encode(void* dst, const void* src, size_t len) {
  auto out = static_cast<char*>(dst);
  auto in = static_cast<char const*>(src);
  for (auto n = len / 3; n--;) {
    *out++ = alphabet[(in[0] & 0xfc) >> 2];
    *out++ = alphabet[((in[0] & 0x03) << 4) + ((in[1] & 0xf0) >> 4)];
    *out++ = alphabet[((in[2] & 0xc0) >> 6) + ((in[1] & 0x0f) << 2)];
    *out++ = alphabet[in[2] & 0x3f];
    in += 3;
  }
  switch (len % 3) {
    case 2:
      *out++ = alphabet[(in[0] & 0xfc) >> 2];
      *out++ = alphabet[((in[0] & 0x03) << 4) + ((in[1] & 0xf0) >> 4)];
      *out++ = alphabet[(in[1] & 0x0f) << 2];
      *out++ = '=';
      break;
    case 1:
      *out++ = alphabet[(in[0] & 0xfc) >> 2];
      *out++ = alphabet[((in[0] & 0x03) << 4)];
      *out++ = '=';
      *out++ = '=';
      break;
    case 0:
      break;
  }
  return out - static_cast<char*>(dst);
}

std::string encode(std::string_view str) {
  std::string result;
  result.resize(encoded_size(str.size()));
  auto n = encode(result.data(), str.data(), str.size());
  result.resize(n);
  return result;
}

std::pair<size_t, size_t> decode(void* dst, char const* src, size_t len) {
  auto out = static_cast<char*>(dst);
  auto in = reinterpret_cast<unsigned char const*>(src);
  unsigned char c3[3], c4[4];
  int i = 0;
  int j = 0;
  while (len-- && *in != '=') {
    auto const v = inverse[*in];
    if (v == -1)
      break;
    ++in;
    c4[i] = v;
    if (++i == 4) {
      c3[0] = (c4[0] << 2) + ((c4[1] & 0x30) >> 4);
      c3[1] = ((c4[1] & 0xf) << 4) + ((c4[2] & 0x3c) >> 2);
      c3[2] = ((c4[2] & 0x3) << 6) + c4[3];
      for (i = 0; i < 3; i++)
        *out++ = c3[i];
      i = 0;
    }
  }
  if (i) {
    c3[0] = (c4[0] << 2) + ((c4[1] & 0x30) >> 4);
    c3[1] = ((c4[1] & 0xf) << 4) + ((c4[2] & 0x3c) >> 2);
    c3[2] = ((c4[2] & 0x3) << 6) + c4[3];
    for (j = 0; j < i - 1; j++)
      *out++ = c3[j];
  }
  return {out - static_cast<char*>(dst),
          in - reinterpret_cast<unsigned char const*>(src)};
}

std::string decode(std::string_view str) {
  std::string result;
  result.resize(decoded_size(str.size()));
  auto [written, read] = decode(result.data(), str.data(), str.size());
  result.resize(written);
  return result;
}

} // namespace vast::detail::base64
