#ifndef VAST_ACTOR_SOURCE_LINE_BASED_H
#define VAST_ACTOR_SOURCE_LINE_BASED_H

#include <cassert>

#include "vast/actor/source/base.h"
#include "vast/io/getline.h"
#include "vast/io/stream.h"

namespace vast {
namespace source {

/// A line-based source that transforms an input stream into lines.
template <typename Derived>
class line_based : public base<Derived>
{
public:
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

protected:
  /// Constructs a a lined-based source.
  /// @param name The name of the actor.
  /// @param is The input stream to read from.
  line_based(char const* name, std::unique_ptr<io::input_stream> is)
    : base<Derived>{name},
      input_stream_{std::move(is)}
  {
    assert(input_stream_ != nullptr);
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
      if (io::getline(*input_stream_, line_))
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

private:
  std::unique_ptr<io::input_stream> input_stream_;
  uint64_t current_ = 0;
  std::string line_;
};

} // namespace source
} // namespace vast

#endif
