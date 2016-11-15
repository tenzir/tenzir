#ifndef VAST_FORMAT_READER_HPP
#define VAST_FORMAT_READER_HPP

#include <istream>
#include <memory>

#include "vast/detail/assert.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/maybe.hpp"

namespace vast {
namespace format {

/// A generic event reader.
template <class Parser>
class reader {
public:
  reader() = default;

  /// Constructs a generic reader.
  /// @param in The stream of logs to read.
  explicit reader(std::unique_ptr<std::istream> in) : in_{std::move(in)} {
    VAST_ASSERT(in_);
    lines_ = std::make_unique<detail::line_range>(*in_);
  }

  maybe<event> read() {
    if (lines_->done())
      return make_error(ec::end_of_input, "input exhausted");
    event e;
    if (!parser_(lines_->get(), e))
      return make_error(ec::parse_error, "line", lines_->line_number());
    lines_->next();
    return e;
  }

protected:
  Parser parser_;

private:
  std::unique_ptr<std::istream> in_;
  std::unique_ptr<detail::line_range> lines_;
};

} // namespace format
} // namespace vast

#endif
