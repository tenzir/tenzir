#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cstdio>
#include <cstring>

#include "vast/detail/assert.hpp"
#include "vast/detail/mmapbuf.hpp"
#include "vast/detail/system.hpp"

namespace vast {
namespace detail {

mmapbuf::mmapbuf() {
  setg(nullptr, nullptr, nullptr);
  setp(nullptr, nullptr);
}

mmapbuf::mmapbuf(size_t size)
  : prot_{PROT_READ | PROT_WRITE},
    flags_{MAP_ANON | MAP_SHARED} {
  VAST_ASSERT(size > 0);
  auto map = mmap(nullptr, size, prot_, flags_, -1, 0);
  if (map == MAP_FAILED)
    return;
  map_ = reinterpret_cast<char_type*>(map);
  size_ = size;
  setg(map_, map_, map_ + size_);
  setp(map_, map_ + size_);
}

mmapbuf::mmapbuf(const path& filename, size_t size, size_t offset)
  : filename_{filename},
    prot_{PROT_READ | PROT_WRITE},
    flags_{MAP_FILE | MAP_SHARED} {
  // Auto-detect file size.
  auto file_size = size_t{0};
  struct stat st;
  auto result = ::stat(filename.str().c_str(), &st);
  if (result == 0)
    file_size = st.st_size;
  else if (result < 0 && errno != ENOENT)
    return;
  // Open/create file and resize if the mapping is larger than the file.
  auto fd = open(filename.str().c_str(), O_RDWR | O_CREAT, 0644);
  if (fd == -1)
    return;
  if (size == 0)
    size = file_size;
  else if (size > file_size)
    if (ftruncate(fd, size) < 0)
      return;
  // Map file into memory.
  auto map = mmap(nullptr, size, prot_, flags_, fd, offset);
  if (map == MAP_FAILED)
    return;
  map_ = reinterpret_cast<char_type*>(map);
  fd_ = fd;
  size_ = size;
  offset_ = offset;
  setp(map_, map_ + size_);
  setg(map_, map_, map_ + size_);
}

mmapbuf::~mmapbuf() {
  if (map_)
    munmap(map_, size_);
  if (fd_ != -1)
    close(fd_);
}

const mmapbuf::char_type* mmapbuf::data() const {
  return map_;
}

size_t mmapbuf::size() const {
  return size_;
}

bool mmapbuf::resize(size_t new_size) {
  // Also fail if we created a private mapping of a file not opened RW.
  // For now, users cannot control these aspects.
  if (!map_ || offset_ > new_size)
    return false;
  if (new_size == size())
    return true;
  // Save current stream buffer positions relative to the beginning.
  size_t get_pos = gptr() - eback();
  size_t put_pos = pptr() - pbase();
  // Resize the underlying file, if available.
  if (fd_ != -1 && ftruncate(fd_, new_size) < 0)
    return false;
  if (new_size < size_) {
    // When shrinking the mapping, we can simply truncate the file underneath
    // the mapping under the assumption that no user accesses the previously
    // allocated memory. While convenient, this approach wastes virtual memory,
    // which could become an issue on 32-bit systems. To relinquish no-longer
    // used frames, we give the OS a hint (which it may ignore) that we're
    // done with them. However, we must be very careful not to evict pages that
    // overlap with the active region, which requires rounding up to the
    // address of the next page.
    auto unused_pages = (size_ - new_size) / page_size();
    if (unused_pages > 0) {
      auto used_pages = (new_size + page_size() - 1) / page_size(); // ceil
      auto used_bytes = used_pages * page_size();
      if (used_bytes < size_)
        madvise(map_ + used_bytes, size_ - used_bytes, MADV_DONTNEED);
    }
  } else {
    // When growing, we have multiple options:
    // (1) Request a mapping immediately after the current mapping to
    //     extend the current mapping in a contiguous fashion.
    // (2) If the OS doesn't allow us (1), we can unmap the existing region
    //     and then create a new one with the new size.
    auto remap = true;
    // If the current mapping is a multiple of the page size, we can try (1).
    if (size_ % page_size() == 0) {
      auto flags = flags_ | MAP_FIXED;
      auto map = mmap(map_ + size_, new_size - size_, prot_, flags, fd_, 0);
      if (map != MAP_FAILED) {
        remap = false; // It worked!
      } else if (errno != ENOMEM) {
        reset();
        return false;
      }
    }
    // If (1) failed or was not possible, we have to resort to (2).
    if (remap) {
      auto map = mmap(nullptr, new_size, prot_, flags_, fd_, offset_);
      if (map == MAP_FAILED) {
        reset();
        return false;
      }
      std::memcpy(map, map_, size_);
      if (munmap(map_, size_) < 0) {
        reset();
        return false;
      }
      map_ = reinterpret_cast<char_type*>(map);
    }
  };
  size_ = new_size;
  // Restore stream buffer positions.
  setp(map_, map_ + size_);
  setg(map_, map_ + std::min(get_pos, size_), map_ + size_);
  pbump(static_cast<int>(std::min(put_pos, size_)));
  return true;
}

chunk_ptr mmapbuf::release() {
  if (!map_)
    return nullptr;
  auto deleter = [](char* map, size_t n) { ::munmap(map, n); };
  auto chk = chunk::make(size_, map_, deleter);
  map_ = nullptr;
  size_ = 0;
  if (fd_ != -1) {
    close(fd_);
    fd_ = -1;
  }
  setp(nullptr, nullptr);
  setg(nullptr, nullptr, nullptr);
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

void mmapbuf::reset() {
  if (map_ != nullptr) {
    munmap(map_, size_);
    map_ = nullptr;
  }
  size_ = 0;
  if (fd_ != -1) {
    close(fd_);
    fd_ = -1;
  }
  setp(nullptr, nullptr);
  setg(nullptr, nullptr, nullptr);
}

} // namespace detail
} // namespace vast
