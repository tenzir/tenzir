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

#include "vast/fbs/utils.hpp"

#include "vast/chunk.hpp"
#include "vast/error.hpp"

#include <optional>
#include <string_view>

namespace vast::fbs {

chunk_ptr release(flatbuffers::FlatBufferBuilder& builder) {
  size_t offset;
  size_t size;
  auto ptr = builder.ReleaseRaw(size, offset);
  auto deleter = [=]() { flatbuffers::DefaultAllocator::dealloc(ptr, size); };
  return chunk::make(size - offset, ptr + offset, deleter);
}

flatbuffers::Verifier make_verifier(span<const byte> xs) {
  auto data = reinterpret_cast<const uint8_t*>(xs.data());
  return flatbuffers::Verifier{data, xs.size()};
}

caf::error check_version(Version given, Version expected) {
  if (given == expected)
    return caf::none;
  return make_error(ec::version_error, "unsupported version;", "got", given,
                    ", expected", expected);
}

span<const byte> as_bytes(const flatbuffers::FlatBufferBuilder& builder) {
  auto data = reinterpret_cast<const byte*>(builder.GetBufferPointer());
  auto size = builder.GetSize();
  return {data, size};
}

std::pair<std::string, Version> resolve_filemagic(span<const byte> fb) {
  using namespace std::string_literals;
  static const std::map<std::string, std::pair<std::string, Version>>
    known_identifiers = {
      {"I000"s, {"vast.fbs.Index"s, Version::v0}},
      {"P000"s, {"vast.fbs.Partition"s, Version::v0}},
      {"S000"s, {"vast.fbs.Segment"s, Version::v0}},
    };
  // The file identifier is 4 bytes long and at a fixed offset 4.
  // Since `GetBufferIdentifier()` doesn't know the buffer size, it might
  // segfault if the buffer is too short.
  if (fb.size() < 8)
    return {"invalid_flatbuffer"s, Version::Invalid};
  // The identifier is used for versioning, since it's the only part of the
  // file we can read without knowing the schema. It's not null-terminated
  // so we have to set the string length explicitly.
  const auto len = flatbuffers::FlatBufferBuilder::kFileIdentifierLength;
  std::string identifier(flatbuffers::GetBufferIdentifier(fb.data()), len);
  auto it = known_identifiers.find(identifier);
  if (it == known_identifiers.end())
    return {"unknown_type:" + identifier, Version::Invalid};
  return it->second;
}

} // namespace vast::fbs
