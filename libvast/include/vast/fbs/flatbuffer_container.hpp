//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// TODO: Should this header be folded into `fbs/utils.hpp`?

#pragma once

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"
#include "vast/fbs/segmented_file.hpp"

namespace vast::fbs {

class flatbuffer_container {
public:
  flatbuffer_container(vast::chunk_ptr chunk);

  // NOTE: Index 0 will return the file chunk
  // to the first offset stored in the table of contents,
  // not the ToC itself.
  chunk_ptr get_raw(size_t idx);

  template <typename T>
  [[nodiscard]] const T* get(size_t idx) const {
    return reinterpret_cast<const T*>(this->get(idx));
  }

  [[nodiscard]] size_t size() const;

  /// Tests is this container was constructed successfully.
  operator bool() const;

private:
  [[nodiscard]] const std::byte* get(size_t idx) const;

  vast::chunk_ptr chunk_ = nullptr;
  vast::fbs::segmented_file::v0 const* toc_ = nullptr;
};

class flatbuffer_container_builder {
public:
  flatbuffer_container_builder(size_t expected_size
                               = 1024ull * 1024 * 1024 * 5 / 2); // 2.5 GiB

  void add(std::span<const std::byte> bytes);

  flatbuffer_container finish() &&;

private:
  // Space for ~640 TOC entries. Given that we only need to use
  // the flatbuffer_container if the total file size is >= 2GiB,
  // the amount wasted here should not matter.
  constexpr static auto PROBABLY_ENOUGH_BYTES_FOR_TOC_FLATBUFFER = 1024ull;

  std::vector<fbs::segmented_file::FileSegment> segments_;
  std::vector<std::byte> file_contents_;
};

} // namespace vast::fbs
