#ifndef VAST_ACTOR_SOURCE_FILE_H
#define VAST_ACTOR_SOURCE_FILE_H

#include "vast/actor/source/synchronous.h"
#include "vast/io/file_stream.h"
#include "vast/io/getline.h"

namespace vast {
namespace source {

/// A file source that transforms file contents into events.
template <typename Derived>
class file : public synchronous<file<Derived>>
{
public:
  /// Constructs a file source.
  /// @param filename The name of the file to ingest.
  file(std::string const& filename)
    : file_handle_{path{filename}},
      file_stream_{file_handle_}
  {
    file_handle_.open(vast::file::read_only);
  }

  result<event> extract()
  {
    return static_cast<Derived*>(this)->extract_impl();
  }

  /// Advances to the next non-empty line in the file.
  /// @returns `true` on success and false on failure or EOF.
  bool next_line()
  {
    if (this->done())
      return false;
    if (! file_handle_.is_open())
    {
      this->done(true);
      return false;
    }
    // Get the next non_empty line.
    line_.clear();
    while (line_.empty())
      if (io::getline(file_stream_, line_))
      {
        ++current_;
      }
      else
      {
        this->done(true);
        return false;
      }
    return true;
  }

  /// Retrieves the current line number.
  uint64_t line_number() const
  {
    return current_;
  }

  /// Retrieves the current line.
  std::string const& line() const
  {
    return line_;
  }

private:
  vast::file file_handle_;
  io::file_input_stream file_stream_;
  uint64_t current_ = 0;
  std::string line_;
};

} // namespace source
} // namespace vast

#endif
