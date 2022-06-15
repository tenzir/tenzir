//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/chunk.hpp"

#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/tracepoint.hpp"
#include "vast/error.hpp"
#include "vast/io/read.hpp"
#include "vast/io/save.hpp"
#include "vast/logger.hpp"

#include <arrow/buffer.h>
#include <arrow/util/compression.h>
#include <caf/deserializer.hpp>
#include <caf/make_counted.hpp>
#include <caf/serializer.hpp>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <span>
#include <tuple>
#include <unistd.h>

namespace vast {

// -- constructors, destructors, and assignment operators ----------------------

chunk::~chunk() noexcept {
  auto data = view_.data();
  auto sz = view_.size();
  VAST_TRACEPOINT(chunk_delete, data, sz);
  if (deleter_)
    std::invoke(deleter_);
}

// -- factory functions ------------------------------------------------------

chunk_ptr
chunk::make(const void* data, size_type size, deleter_type&& deleter) noexcept {
  return make(view_type{static_cast<pointer>(data), size}, std::move(deleter));
}

chunk_ptr chunk::make(view_type view, deleter_type&& deleter) noexcept {
  return chunk_ptr{new chunk{view, std::move(deleter)}, false};
}

chunk_ptr chunk::make_empty() noexcept {
  return chunk_ptr{new chunk{view_type{}, deleter_type{}}, false};
}

caf::expected<chunk_ptr> chunk::mmap(const std::filesystem::path& filename,
                                     size_type size, size_type offset) {
  // Figure out the file size if not provided.
  if (size == 0) {
    std::error_code err{};
    size = std::filesystem::file_size(filename, err);
    if (size == static_cast<std::uintmax_t>(-1))
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to get file size for file"
                                         "{}: {}",
                                         filename, err.message()));
  }
  // Open and memory-map the file.
  auto fd = ::open(filename.c_str(), O_RDONLY, 0644);
  if (fd == -1)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to open file {}", filename));
  auto map = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, offset);
  auto mmap_errno = errno;
  ::close(fd);
  if (map == MAP_FAILED)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to mmap file {}: {}", filename,
                                       std::strerror(mmap_errno)));
  auto deleter = [=]() noexcept {
    ::munmap(map, size);
  };
  return make(map, size, std::move(deleter));
}

caf::expected<chunk_ptr> chunk::compress(view_type bytes) noexcept {
  // Creating the codec cannot fail; we test that it works in VAST's main
  // function to catch this early.
  auto codec
    = arrow::util::Codec::Create(
        arrow::Compression::ZSTD,
        arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD)
          .ValueOrDie())
        .ValueOrDie();
  const auto bytes_size = detail::narrow_cast<int64_t>(bytes.size());
  const auto* bytes_data = reinterpret_cast<const uint8_t*>(bytes.data());
  const auto max_length = codec->MaxCompressedLen(bytes_size, bytes_data);
  auto buffer = std::vector<uint8_t>{};
  buffer.resize(max_length);
  auto length
    = codec->Compress(bytes_size, bytes_data, max_length, buffer.data());
  if (!length.ok())
    return caf::make_error(ec::system_error,
                           fmt::format("failed to compress chunk: {}",
                                       length.status().ToString()));
  buffer.resize(length.MoveValueUnsafe());
  return chunk::make(std::move(buffer));
}

caf::expected<chunk_ptr>
chunk::decompress(view_type bytes, size_t decompressed_size) noexcept {
  // Creating the codec cannot fail; we test that it works in VAST's main
  // function to catch this early.
  auto codec
    = arrow::util::Codec::Create(
        arrow::Compression::ZSTD,
        arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD)
          .ValueOrDie())
        .ValueOrDie();
  const auto bytes_size = detail::narrow_cast<int64_t>(bytes.size());
  const auto* bytes_data = reinterpret_cast<const uint8_t*>(bytes.data());
  auto buffer = std::vector<uint8_t>{};
  buffer.resize(decompressed_size);
  auto length = codec->Decompress(bytes_size, bytes_data,
                                  detail::narrow_cast<int64_t>(buffer.size()),
                                  buffer.data());
  if (!length.ok())
    return caf::make_error(ec::system_error,
                           fmt::format("failed to decompress chunk: {}",
                                       length.status().ToString()));
  VAST_ASSERT(buffer.size() == decompressed_size);
  return chunk::make(std::move(buffer));
}

// -- container facade ---------------------------------------------------------

chunk::pointer chunk::data() const noexcept {
  return view_.data();
}

chunk::size_type chunk::size() const noexcept {
  return view_.size();
}

caf::expected<chunk::size_type> chunk::incore() const noexcept {
#if VAST_LINUX || VAST_BSD || VAST_MACOS
  auto sz = sysconf(_SC_PAGESIZE);
  auto pages = (size() / sz) + !!(size() % sz);
#  if VAST_LINUX
  auto buf = std::vector(pages, static_cast<unsigned char>(0));
#  else
  auto buf = std::vector(pages, '\0');
#  endif
  if (mincore(const_cast<value_type*>(data()), size(), buf.data()))
    return caf::make_error(ec::system_error,
                           "failed in mincore(2):", std::strerror(errno));
  auto in_memory = std::accumulate(buf.begin(), buf.end(), 0ul,
                                   [](auto acc, auto current) {
                                     return acc + (current & 0x1);
                                   })
                   * sz;
  return in_memory;
#else
  return caf::make_error(ec::unimplemented);
#endif
}

chunk::iterator chunk::begin() const noexcept {
  return view_.begin();
}

chunk::iterator chunk::end() const noexcept {
  return view_.end();
}

// -- accessors ----------------------------------------------------------------

chunk_ptr chunk::slice(size_type start, size_type length) const {
  VAST_ASSERT(start < size());
  if (length > size() - start)
    length = size() - start;
  return slice(view_.subspan(start, length));
}

chunk_ptr chunk::slice(view_type view) const {
  VAST_ASSERT(view.begin() >= begin());
  VAST_ASSERT(view.end() <= end());
  this->ref();
  return make(view, [this]() noexcept {
    this->deref();
  });
}

// -- free functions ----------------------------------------------------------

std::shared_ptr<arrow::Buffer> as_arrow_buffer(chunk_ptr chunk) noexcept {
  if (!chunk)
    return nullptr;
  auto buffer = arrow::Buffer::Wrap(chunk->data(), chunk->size());
  VAST_ASSERT(reinterpret_cast<const std::byte*>(buffer->data())
              == chunk->data());
  auto* const buffer_data = buffer.get();
  return {buffer_data, [chunk = std::move(chunk), buffer = std::move(buffer)](
                         arrow::Buffer*) mutable noexcept {
            // We manually call the destructors in proper order here, as the
            // chunk must be destroyed last and the destruction order for lambda
            // captures is undefined.
            buffer = {};
            chunk = {};
          }};
}

// -- concepts -----------------------------------------------------------------

std::span<const std::byte> as_bytes(const chunk_ptr& x) noexcept {
  if (!x)
    return {};
  return as_bytes(*x);
}

caf::error write(const std::filesystem::path& filename, const chunk_ptr& x) {
  return io::save(filename, as_bytes(x));
}

caf::error read(const std::filesystem::path& filename, chunk_ptr& x) {
  std::error_code err{};
  const auto size = std::filesystem::file_size(filename, err);
  if (size == static_cast<std::uintmax_t>(-1)) {
    x = nullptr;
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to get file size for filename "
                                       "{}: {}",
                                       filename, err.message()));
  }
  auto buffer = std::make_unique<chunk::value_type[]>(size);
  auto view = std::span{buffer.get(), size};
  if (auto err = io::read(filename, view)) {
    x = nullptr;
    return err;
  }
  x = chunk::make(view, [buffer = std::move(buffer)]() noexcept {
    static_cast<void>(buffer);
  });
  return caf::none;
}

caf::error inspect(caf::serializer& sink, const chunk_ptr& x) {
  using vast::detail::narrow;
  if (x == nullptr)
    return sink(uint32_t{0});
  return caf::error::eval(
    [&] {
      return sink(narrow<uint32_t>(x->size()));
    },
    [&] {
      return sink.apply_raw(x->size(), const_cast<std::byte*>(x->data()));
    });
}

caf::error inspect(caf::deserializer& source, chunk_ptr& x) {
  uint32_t size = 0;
  if (auto err = source(size))
    return err;
  if (size == 0) {
    x = nullptr;
    return caf::none;
  }
  auto buffer = std::make_unique<chunk::value_type[]>(size);
  const auto data = buffer.get();
  if (auto err = source.apply_raw(size, data)) {
    x = nullptr;
    return err;
  }
  x = chunk::make(data, size, [buffer = std::move(buffer)]() noexcept {
    static_cast<void>(buffer);
  });
  return caf::none;
}

bool inspect(detail::legacy_deserializer& source, chunk_ptr& x) {
  uint32_t size = 0;
  if (!source(size))
    return false;
  if (size == 0) {
    x = nullptr;
    return true;
  }
  auto buffer = std::make_unique<chunk::value_type[]>(size);
  const auto data = buffer.get();
  if (!source.apply_raw(size, data)) {
    x = nullptr;
    return false;
  }
  x = chunk::make(data, size, [buffer = std::move(buffer)]() noexcept {
    static_cast<void>(buffer);
  });
  return true;
}

// -- implementation details ---------------------------------------------------

chunk::chunk(view_type view, deleter_type&& deleter) noexcept
  : view_{view}, deleter_{std::move(deleter)} {
  auto data = view.data();
  auto sz = view.size();
  VAST_TRACEPOINT(chunk_make, data, sz);
}

} // namespace vast
