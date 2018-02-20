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

#ifndef VAST_DETAIL_MMAPBUF_HPP
#define VAST_DETAIL_MMAPBUF_HPP

#include <cstddef>
#include <streambuf>
#include <string>

#include "vast/chunk.hpp"
#include "vast/filesystem.hpp"

namespace vast::detail {

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
  /// @param offset The offset where to begin mapping; same as in `mmap(2)`.
  explicit mmapbuf(const path& filename, size_t size = 0,
                   size_t offset = 0);

  /// Closes the opened file and unmaps the mapped memory region.
  ~mmapbuf();

  /// Exposes the underlying memory region.
  const char_type* data() const;

  /// @returns the size of the mapped memory region.
  size_t size() const;

  /// Resizes the underlying file and re-maps the file.
  /// @param new_size The new size of the underlying file.
  /// @returns `true` on success.
  bool resize(size_t new_size);

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
  void reset();

  path filename_;
  int fd_ = -1;
  size_t size_ = 0;
  size_t offset_ = 0;
  int prot_ = 0;
  int flags_ = 0;
  char_type* map_ = nullptr;
};

} // namespace vast::detail

#endif
