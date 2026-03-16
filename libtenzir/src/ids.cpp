//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ids.hpp"

#include "tenzir/table_slice.hpp"

namespace tenzir {

ids make_ids(std::initializer_list<id_range> ranges, size_t min_size,
             bool default_bit) {
  ids result;
  for (auto [first, last] : ranges) {
    TENZIR_ASSERT(first < last);
    if (first >= result.size()) {
      result.append_bits(false, first - result.size());
      result.append_bits(true, (last - first));
    } else {
      ids tmp;
      tmp.append_bits(false, first);
      tmp.append_bits(true, (last - first));
      result |= tmp;
    }
  }
  if (result.size() < min_size)
    result.append_bits(default_bit, min_size - result.size());
  return result;
}

ids make_ids(const table_slice& slice) {
  return make_ids({{slice.offset(), slice.offset() + slice.rows()}});
}

} // namespace tenzir
