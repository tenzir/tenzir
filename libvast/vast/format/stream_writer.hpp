#ifndef VAST_FORMAT_STREAM_WRITER_HPP
#define VAST_FORMAT_STREAM_WRITER_HPP

#include <iterator>
#include <memory>
#include <ostream>

#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"

#include "vast/format/writer.hpp"

namespace vast {
namespace format {

/// A generic writer that writes events into an I/O stream, after rendering it
/// according to a specific printer..
template <class Printer>
class stream_writer : public writer<stream_writer<Printer>> {
public:
  stream_writer() = default;

  /// Constructs a stream writer.
  /// @param out The stream where to write to
  explicit stream_writer(std::unique_ptr<std::ostream> out)
    : out_{std::move(out)} {
  }

  expected<void> process(event const& e) {
    auto i = std::ostreambuf_iterator<char>(*out_);
    if (!printer_.print(i, e))
      return make_error(ec::print_error, "failed to print event:", e);
    *out_ << '\n';
    return {};
  }

  expected<void> flush() {
    out_->flush();
    if (!*out_)
      return make_error(ec::format_error, "failed to flush");
    return {};
  }

private:
  std::unique_ptr<std::ostream> out_;
  Printer printer_;
};

} // namespace format
} // namespace vast

#endif

