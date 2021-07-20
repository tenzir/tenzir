//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/fbs/utils.hpp"

#include "vast/chunk.hpp"
#include "vast/error.hpp"

#include <cstddef>
#include <optional>
#include <span>
#include <string_view>

namespace vast::fbs {

chunk_ptr release(flatbuffers::FlatBufferBuilder& builder) {
  // A previous version of this function manually deleted the buffer in the
  // deleter with `flatbuffers::DefaultAllocator::dealloc(...)` after using
  // `builder.ReleaseRaw(...)`, which is not guaranteed to be safe. The detached
  // buffer returned by `builder.Release()` deletes the buffer in its destructor
  // with the correct allocator. This ensures that the chunk can correctly
  // release the memory even if a non-default allocator is used.
  return chunk::make(builder.Release());
}

flatbuffers::Verifier make_verifier(std::span<const std::byte> xs) {
  auto data = reinterpret_cast<const uint8_t*>(xs.data());
  return flatbuffers::Verifier{data, xs.size()};
}

} // namespace vast::fbs
