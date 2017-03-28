#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <tuple>

#include "vast/chunk.hpp"

#include "vast/detail/assert.hpp"

namespace vast {

chunk_ptr chunk::mmap(const std::string& filename, size_t size, size_t offset) {
  // Figure out the file size if not provided.
  if (size == 0) {
    struct stat st;
    auto result = ::stat(filename.c_str(), &st);
    if (result == -1)
      return {};
    size = st.st_size;
  }
  // Open and memory-map the file.
  auto fd = ::open(filename.c_str(), O_RDONLY, 0644);
  if (fd == -1)
    return {};
  auto map = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, offset);
  ::close(fd);
  if (map == MAP_FAILED)
    return {};
  // Construct a chunk from it that unmaps itself upon deletion.
  auto deleter = [](char* buf, size_t n) {
    ::munmap(buf, n);
  };
  return make(size, reinterpret_cast<char*>(map), deleter);
}

chunk::~chunk() {
  if (deleter_)
    deleter_(data_, size_);
}

const char* chunk::data() const {
  return data_;
}

size_t chunk::size() const {
  return size_;
}

chunk::chunk(size_t size)
  : data_{new char[size]},
    size_{size},
    deleter_{[](char* ptr, size_t) { delete[] ptr; }} {
  VAST_ASSERT(size_ > 0);
}

chunk::chunk(size_t size, void* ptr)
  : data_{reinterpret_cast<char*>(ptr)},
    size_{size} {
}

bool operator==(const chunk& x, const chunk& y) {
  return x.data() == y.data() && x.size() == y.size();
}

bool operator<(const chunk& x, const chunk& y) {
  auto lhs = std::make_tuple(x.data(), x.size());
  auto rhs = std::make_tuple(y.data(), y.size());
  return lhs < rhs;
}

} // namespace vast
