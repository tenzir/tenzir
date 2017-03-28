#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cstdio>
#include <cstring>

#include "vast/detail/assert.hpp"
#include "vast/detail/mmapbuf.hpp"

namespace vast {
namespace detail {

mmapbuf::mmapbuf(const std::string& filename, size_t size, size_t offset)
  : size_{size} {
  if (size == 0) {
    struct stat st;
    auto result = ::stat(filename.c_str(), &st);
    if (result == -1)
      return;
    size_ = st.st_size;
  }
  fd_ = ::open(filename.c_str(), O_RDWR, 0644);
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
  VAST_ASSERT(pos < static_cast<pos_type>(size_));
  if (which == std::ios_base::in)
    setg(map_, map_ + pos, map_ + size_);
  if (which == std::ios_base::out)
    setp(map_ + pos, map_ + size_);
  return pos;
}

} // namespace detail
} // namespace vast
