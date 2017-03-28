#ifndef VAST_DETAIL_MMAPBUF_HPP
#define VAST_DETAIL_MMAPBUF_HPP

#include <cstddef>
#include <streambuf>
#include <string>

namespace vast {
namespace detail {

/// A memory-mapped streambuffer. The put and get areas corresponds to the
/// mapped memory region.
class mmapbuf : public std::streambuf {
public:
  /// Constructs a memory-mapped streambuffer from a file.
  /// @param filename The path to the file to open.
  /// @param size The size of the file in bytes. If 0, figure out file size
  ///             automatically.
  /// @param offset An offset where to begin mapping.
  /// @pre `offset < size`
  explicit mmapbuf(const std::string& filename, size_t size = 0,
                   size_t offset = 0);

  /// Closes the opened file and unmaps the mapped memory region.
  ~mmapbuf();

  /// Exposes the underlying memory region.
  const char_type* data() const;

  /// Returns the size of the mapped memory region.
  size_t size() const;

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
  size_t size_;
  char_type* map_ = nullptr;
};

} // namespace detail
} // namespace vast

#endif
