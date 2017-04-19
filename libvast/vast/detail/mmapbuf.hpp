#ifndef VAST_DETAIL_MMAPBUF_HPP
#define VAST_DETAIL_MMAPBUF_HPP

#include <cstddef>
#include <streambuf>
#include <string>

#include "vast/chunk.hpp"

namespace vast {

class path;

namespace detail {

/// A memory-mapped stream buffer. The put and get areas corresponds to the
/// mapped memory region.
class mmapbuf : public std::streambuf {
public:
  /// Default-constructs an empty memory-mapped buffer.
  mmapbuf();

  /// Constructs an anonymous memory-mapped stream buffer.
  /// @param size The size of the mapped region.
  /// @pre `size > 0`
  explicit mmapbuf(size_t size);

  /// Constructs a file-backed memory-mapped stream buffer.
  /// @param filename The path to the file to open.
  /// @param size The size of the file in bytes. If 0, figure out file size
  ///             automatically.
  /// @param offset An offset where to begin mapping.
  explicit mmapbuf(const path& filename, size_t size = 0,
                   size_t offset = 0);

  /// Closes the opened file and unmaps the mapped memory region.
  ~mmapbuf();

  /// Checks whether the memory map is valid.
  /// @returns `true` if the mapping returned by `mmap` is valid.
  explicit operator bool() const;

  /// Exposes the underlying memory region.
  const char_type* data() const;

  /// Returns the size of the mapped memory region.
  size_t size() const;

  /// Truncates the underlying file to a given size.
  /// @param new_size The new size of the underlying file.
  /// @returns `true` on success.
  bool truncate(size_t new_size);

  /// Release the underlying memory region. Subsequent operations on the stream
  /// evoke undefined behavior
  /// @returns A chunk representing the mapped memory region.
  chunk_ptr release();

protected:
  // -- get area --------------------------------------------------------------

  std::streamsize showmanyc() override;

  std::streamsize xsgetn(char_type* s, std::streamsize n) override;

  // -- put area --------------------------------------------------------------

  std::streamsize xsputn(const char_type* s, std::streamsize n) override;

  // -- positioning -----------------------------------------------------------

  pos_type seekoff(off_type off,
                   std::ios_base::seekdir dir,
                   std::ios_base::openmode which =
                     std::ios_base::in | std::ios_base::out) override;

  pos_type seekpos(pos_type pos,
                   std::ios_base::openmode which =
                     std::ios_base::in | std::ios_base::out) override;

private:
  int fd_ = -1;
  size_t size_ = 0;
  char_type* map_ = nullptr;
};

} // namespace detail
} // namespace vast

#endif
