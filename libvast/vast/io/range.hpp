#ifndef VAST_IO_RANGE_HPP
#define VAST_IO_RANGE_HPP

#include "vast/io/buffer.hpp"
#include "vast/io/stream.hpp"
#include "vast/util/range.hpp"

namespace vast {
namespace io {

class input_stream_range : public util::range_facade<input_stream_range> {
public:
  input_stream_range(input_stream& is) : is_{&is} {
    next();
  }

protected:
  bool next() {
    buf_ = is_->next_block();
    return !!buf_;
  }

private:
  friend util::range_facade<input_stream_range>;

  buffer<void const> const& state() const {
    return buf_;
  }

  buffer<void const> buf_;
  input_stream* is_;
};

} // namespace io
} // namespace vast

#endif
