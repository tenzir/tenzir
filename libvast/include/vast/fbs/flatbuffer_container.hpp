//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"
#include "vast/fbs/segmented_file.hpp"

namespace vast::fbs {

/// This container provides a `std::vector<vast::chunk>`-like
/// interface for chunks that were created from
/// a `flatbuffer_container_builder`.
// The typical usage is for the first chunk to be some flatbuffer
// and the subsequent ones being data blobs that are too big to
// store inline without hitting the 2GiB limit for individual
// flatbuffers.
class flatbuffer_container {
public:
  // The `chunk` must begin with a flatbuffer that has
  // `fbs::SegmentedFileHeader` as root type.
  explicit flatbuffer_container(vast::chunk_ptr chunk);

  /// Get the chunk at position `idx`.
  chunk_ptr get_raw(size_t idx) const;

  /// Return the chunk at position `idx` interpreted as a `T`.
  template <typename T>
    requires std::is_trivial_v<T>
  [[nodiscard]] const T* as(size_t idx) const {
    return reinterpret_cast<const T*>(this->get(idx));
  }

  /// Return the chunk at position `idx` interpreted as a root
  /// flatbuffer of type `T`.
  //  (the difference to `as<T>()` is that the optional 4-bytes
  //  buffer identifier at the beginning is handled correctly)
  //  TODO: This should probably return a `flatbuffer<T>` so the
  //  caller can save a copy if he wants to store the flatbuffer.
  template <typename T>
  [[nodiscard]] const T* as_flatbuffer(size_t idx) const {
    return flatbuffers::GetRoot<T>(this->get(idx));
  }

  /// Number of elements in the container.
  [[nodiscard]] size_t size() const;

  /// Tests is this container was constructed successfully.
  operator bool() const;

  vast::chunk_ptr chunk() const;

  /// Give up ownership of the chunk and clear this container.
  vast::chunk_ptr dissolve() &&;

private:
  [[nodiscard]] const std::byte* get(size_t idx) const;

  vast::chunk_ptr chunk_ = nullptr;
  vast::fbs::segmented_file::v0 const* header_ = nullptr;
};

class flatbuffer_container_builder {
public:
  flatbuffer_container_builder(size_t expected_size
                               = 1024ull * 1024 * 1024 * 5 / 2); // 2.5 GiB

  void add(std::span<const std::byte> bytes);

  flatbuffer_container finish(const char* identifier) &&;

private:
  // Space for ~640 TOC entries. Given that we only need to use
  // the flatbuffer_container if the total file size is >= 2GiB,
  // the amount wasted here should not matter.
  constexpr static auto PROBABLY_ENOUGH_BYTES_FOR_TOC_FLATBUFFER = 1024ull;

  std::vector<fbs::segmented_file::FileSegment> segments_;
  std::vector<std::byte> file_contents_;
};

} // namespace vast::fbs
