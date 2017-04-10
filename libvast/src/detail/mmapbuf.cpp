#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cstdio>
#include <cstring>

#include "vast/filesystem.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/mmapbuf.hpp"

namespace vast {
namespace detail {

mmapbuf::mmapbuf() {
  setg(nullptr, nullptr, nullptr);
  setp(nullptr, nullptr);
}

mmapbuf::mmapbuf(size_t size) : size_{size} {
  VAST_ASSERT(size > 0);
  auto prot = PROT_READ | PROT_WRITE;
  auto map = ::mmap(nullptr, size_, prot, MAP_ANON | MAP_SHARED, -1, 0);
  if (map == MAP_FAILED)
    return;
  map_ = reinterpret_cast<char_type*>(map);
  setg(map_, map_, map_ + size_);
  setp(map_, map_ + size_);
}

mmapbuf::mmapbuf(const path& filename, size_t size, size_t offset)
  : size_{size} {
  if (size == 0) {
    struct stat st;
    auto result = ::stat(filename.str().c_str(), &st);
    if (result == -1 || st.st_size == 0)
      return;
    size_ = st.st_size;
  }
  VAST_ASSERT(size_ > 0);
  auto fd_ = ::open(filename.str().c_str(), O_RDWR, 0644);
  if (fd_ == -1)
    return;
  auto prot = PROT_READ | PROT_WRITE;
  auto map = ::mmap(nullptr, size_, prot, MAP_SHARED, fd_, offset);
  if (map == MAP_FAILED)
    return;
  map_ = reinterpret_cast<char_type*>(map);
  setg(map_, map_, map_ + size_);
  setp(map_, map_ + size_);
}

mmapbuf::~mmapbuf() {
  if (!map_)
    ::munmap(map_, size_);
  if (fd_ != -1)
    ::close(fd_);
}

const mmapbuf::char_type* mmapbuf::data() const {
  return map_;
}

size_t mmapbuf::size() const {
  return size_;
}

// We could inform the OS that we don't need the unused pages any more by
// calling something along the lines of:
//
//   madvise(map_ + new_size, map_ + new_size - size_, MADV_DONT_NEED);
//
// However, we must be very careful not to evict pages that overlap with the
// active region, which requires rounding up to the address of next page.
bool mmapbuf::truncate(size_t new_size) {
  if (new_size >= size())
    return false;
  // Save stream buffer positions.
  size_t get_pos = gptr() - eback();
  size_t put_pos = pptr() - pbase();
  // Truncate file.
  if (fd_ != -1 && ::ftruncate(fd_, new_size) != 0)
    return false;
  size_ = new_size;
  // Restore stream buffer positions.
  setg(map_, map_ + std::min(get_pos, size_), map_ + size_);
  setp(map_, map_ + size_);
  pbump(static_cast<int>(std::min(put_pos, size_)));
  return true;
}

chunk_ptr mmapbuf::release() {
  auto deleter = [](char* map, size_t n) { ::munmap(map, n); };
  auto chk = chunk::make(size_, map_, deleter);
  map_ = nullptr;
  size_ = 0;
  if (fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
  }
  setg(nullptr, nullptr, nullptr);
  setp(nullptr, nullptr);
  return chk;
}

std::streamsize mmapbuf::showmanyc() {
  VAST_ASSERT(map_);
  return egptr() - gptr();
}

std::streamsize mmapbuf::xsgetn(char_type* s, std::streamsize n) {
  VAST_ASSERT(map_);
  n = std::min(n, egptr() - gptr());
  std::memcpy(s, gptr(), n);
  gbump(n);
  return n;
}

std::streamsize mmapbuf::xsputn(const char_type* s, std::streamsize n) {
  VAST_ASSERT(map_);
  n = std::min(n, epptr() - pptr());
  std::memcpy(pptr(), s, n);
  pbump(n);
  return n;
}

mmapbuf::pos_type mmapbuf::seekoff(off_type off, std::ios_base::seekdir dir,
                                   std::ios_base::openmode which) {
  VAST_ASSERT(map_);
  // We cannot reposition put and get area simultaneously because the return
  // value would be meaningless.
  if ((which & std::ios_base::in) && (which & std::ios_base::out))
    return -1;
  switch (dir) {
    default:
      return -1;
    case std::ios_base::beg:
      return seekpos(off, which);
    case std::ios_base::cur:
      if (which == std::ios_base::in)
        return seekpos(gptr() - eback() + off, std::ios_base::in);
      else
        return seekpos(pptr() - pbase() + off, std::ios_base::out);
    case std::ios_base::end:
      return seekpos(size_ + off, which);
  }
}

mmapbuf::pos_type mmapbuf::seekpos(pos_type pos,
                                   std::ios_base::openmode which) {
  VAST_ASSERT(map_);
  VAST_ASSERT(pos <= static_cast<pos_type>(size_));
  if (which == std::ios_base::in)
    setg(map_, map_ + pos, map_ + size_);
  if (which == std::ios_base::out)
    setp(map_ + pos, map_ + size_);
  return pos;
}

} // namespace detail
} // namespace vast
