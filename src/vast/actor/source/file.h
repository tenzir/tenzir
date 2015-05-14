#ifndef VAST_ACTOR_SOURCE_FILE_H
#define VAST_ACTOR_SOURCE_FILE_H

#include "vast/actor/source/synchronous.h"
#include "vast/io/file_stream.h"
#include "vast/io/getline.h"

namespace vast {
namespace source {

/// A line-based source that transforms an input stream into lines.
template <typename Derived>
class file : public synchronous<Derived>
{
public:
  /// Constructs a file source.
  /// @param name The name of the actor.
  /// @param f The file to ingest.
  file(char const* name, io::file_input_stream&& stream)
    : synchronous<Derived>{name},
      stream_{std::move(stream)}
  {
  }

  /// Advances to the next non-empty line in the file.
  /// @returns `true` on success and false on failure or EOF.
  bool next_line()
  {
    if (this->done())
      return false;
    line_.clear();
    // Get the next non-empty line.
    while (line_.empty())
      if (io::getline(stream_, line_))
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
  io::file_input_stream stream_;
  uint64_t current_ = 0;
  std::string line_;
};

} // namespace source
} // namespace vast

#endif
