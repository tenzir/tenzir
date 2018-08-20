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

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <tuple>

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include "vast/chunk.hpp"
#include "vast/filesystem.hpp"

#include "vast/detail/assert.hpp"

namespace vast {

chunk_ptr chunk::mmap(const path& filename, size_t size, size_t offset) {
  // Figure out the file size if not provided.
  if (size == 0) {
    struct stat st;
    auto result = ::stat(filename.str().c_str(), &st);
    if (result == -1)
      return {};
    size = st.st_size;
  }
  // Open and memory-map the file.
  auto fd = ::open(filename.str().c_str(), O_RDONLY, 0644);
  if (fd == -1)
    return {};
  auto map = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, offset);
  ::close(fd);
  if (map == MAP_FAILED)
    return {};
  auto deleter = [](char* buf, size_t n) { ::munmap(buf, n); };
  return make(size, reinterpret_cast<char*>(map), deleter);
}

chunk::~chunk() {
  VAST_ASSERT(deleter_);
  deleter_(data_, size_);
}

char* chunk::data() {
  return data_;
}

const char* chunk::data() const {
  return data_;
}

size_t chunk::size() const {
  return size_;
}

chunk::const_iterator chunk::begin() const {
  return data_;
}

chunk::const_iterator chunk::end() const {
  return data_ + size_;
}

chunk_ptr chunk::slice(size_t start, size_t length) const {
  VAST_ASSERT(start + length < size());
  if (length == 0)
    length = size() - start;
  auto self = const_cast<chunk*>(this); // Atomic ref-counting is fine.
  self->ref();
  auto deleter = [=](char*, size_t) { self->deref(); };
  return make(length, data_ + start, std::move(deleter));
}

chunk::chunk(size_t size)
  : data_{new char[size]},
    size_{size},
    deleter_{[](char* ptr, size_t) { delete[] ptr; }} {
  VAST_ASSERT(size > 0);
}

chunk::chunk(size_t size, void* ptr, deleter_type deleter)
  : data_{reinterpret_cast<char*>(ptr)},
    size_{size},
    deleter_{std::move(deleter)} {
}

caf::error inspect(caf::serializer& sink, const chunk_ptr& x) {
  VAST_ASSERT(x != nullptr);
  auto n = x->size();
  return caf::error::eval(
    [&] { return sink.begin_sequence(n); },
    [&] { return n > 0 ? sink.apply_raw(n, x->data()) : caf::none; },
    [&] { return sink.end_sequence(); }
  );
}

caf::error inspect(caf::deserializer& source, chunk_ptr& x) {
  chunk::size_type n;
  return caf::error::eval(
    [&] { return source.begin_sequence(n); },
    [&] {
      x = chunk::make(n);
      return n > 0 ? source.apply_raw(x->size(), x->data()) : caf::none;
    },
    [&] { return source.end_sequence(); }
  );
}

} // namespace vast
