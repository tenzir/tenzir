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
#include <caf/make_counted.hpp>
#include <caf/serializer.hpp>

#include "vast/chunk.hpp"
#include "vast/filesystem.hpp"

#include "vast/detail/narrow.hpp"

namespace vast {

chunk_ptr chunk::make(size_type size) {
  VAST_ASSERT(size > 0);
  auto data = new value_type[size];
  auto deleter = [=] { delete[] data; };
  return make(size, data, std::move(deleter));
}

chunk_ptr chunk::make(size_type size, void* data, deleter_type deleter) {
  VAST_ASSERT(size > 0);
  return chunk_ptr{new chunk{data, size, deleter}, false};
}

chunk_ptr chunk::mmap(const path& filename, size_type size, size_type offset) {
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
  auto deleter = [=] { ::munmap(map, size); };
  return make(size, reinterpret_cast<value_type*>(map), deleter);
}

chunk::~chunk() {
  deleter_();
}

chunk::const_pointer chunk::data() const {
  return data_;
}

chunk::size_type chunk::size() const {
  return size_;
}

chunk::const_iterator chunk::begin() const {
  return data_;
}

chunk::const_iterator chunk::end() const {
  return data_ + size_;
}

chunk::value_type chunk::operator[](size_type i) const {
  VAST_ASSERT(i < size());
  return data_[i];
}

chunk_ptr chunk::slice(size_type start, size_type length) const {
  VAST_ASSERT(start + length < size());
  if (length == 0)
    length = size() - start;
  auto self = const_cast<chunk*>(this); // Atomic ref-counting is fine.
  self->ref();
  auto deleter = [=]() { self->deref(); };
  return make(length, data_ + start, deleter);
}

chunk::chunk(void* ptr, size_type size, deleter_type deleter)
  : data_{reinterpret_cast<value_type*>(ptr)},
    size_{size},
    deleter_{deleter} {
  VAST_ASSERT(deleter_);
}

caf::error inspect(caf::serializer& sink, const chunk_ptr& x) {
  using vast::detail::narrow;
  if (x == nullptr)
    return sink(uint32_t{0});
  auto data = const_cast<chunk::pointer>(x->data());
  return caf::error::eval([&] { return sink(narrow<uint32_t>(x->size())); },
                          [&] { return sink.apply_raw(x->size(), data); });
}

caf::error inspect(caf::deserializer& source, chunk_ptr& x) {
  uint32_t n;
  if (auto err = source(n))
    return err;
  if (n == 0) {
    x = nullptr;
    return caf::none;
  }
  x = chunk::make(n);
  auto data = const_cast<chunk::pointer>(x->data());
  return source.apply_raw(n, data);
}

} // namespace vast
