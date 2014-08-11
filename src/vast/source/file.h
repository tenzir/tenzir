#ifndef VAST_SOURCE_FILE_H
#define VAST_SOURCE_FILE_H

#include "vast/io/file_stream.h"
#include "vast/io/getline.h"
#include "vast/source/synchronous.h"

namespace vast {

class event;

namespace source {

/// A file source that transforms file contents into events.
template <typename Derived>
class file : public synchronous<file<Derived>>
{
public:
  /// Constructs a file source.
  /// @param sink The actor to send the generated events to.
  /// @param filename The name of the file to ingest.
  file(caf::actor sink, std::string const& filename)
    : synchronous<file<Derived>>{std::move(sink)},
      file_handle_{path{filename}},
      file_stream_{file_handle_}
  {
    file_handle_.open(vast::file::read_only);
  }

  result<event> extract()
  {
    return static_cast<Derived*>(this)->extract_impl();
  }

  bool done() const
  {
    return current_line() == nullptr;
  }

  /// Retrieves the next non-empty line from the file.
  ///
  /// @returns A pointer to the next line if extracting was successful and
  /// `nullptr` on failure or EOF.
  std::string const* next()
  {
    if (! file_handle_.is_open())
      return nullptr;

    line_.clear();
    while (line_.empty())
      if (io::getline(file_stream_, line_))
        ++current_;
      else
        return nullptr;

    return &line_;
  }

  uint64_t line_number() const
  {
    return current_;
  }

  std::string const* current_line() const
  {
    return line_.empty() ? nullptr : &line_;
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
