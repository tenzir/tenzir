#ifndef VAST_SINK_STREAM_H
#define VAST_SINK_STREAM_H

#include "vast/file_system.h"
#include "vast/io/buffer.h"
#include "vast/io/file_stream.h"

namespace vast {
namespace sink {

/// A small wrapper around a file output stream.
class stream
{
public:
  /// Constructs a stream from a path.
  /// @param p The output path. If `-` then write the events to STDOUT.
  /// Otherwise *p* must not exist or point to an existing directory.
  stream(path p)
    : file_{std::move(p)},
      stream_{file_}
  {
    file_.open(file::write_only);
  }

  /// Flushes the underlying file stream.
  /// @returns `true` on success.
  bool flush()
  {
    return file_.is_open() ? stream_.flush() : false;
  }

  /// Writes data into the file.
  /// @tparam An input iterator.
  /// @param begin The beginning of the data.
  /// @param end The end of the data.
  template <typename Iterator>
  bool write(Iterator begin, Iterator end)
  {
    if (! file_.is_open())
      return false;

    auto buf = stream_.next_block();

    if (! buf)
      return false;

    while (begin != end)
    {
      size_t input_size = end - begin;
      if (input_size <= buf.size())
      {
        std::copy(begin, end, buf.data());
        stream_.rewind(buf.size() - input_size);
        break;
      }
      else if (buf.size() == 0)
      {
        return false;
      }
      else
      {
        std::copy(begin, begin + buf.size(), buf.data());
        begin += buf.size();
        buf = stream_.next_block();
        if (! buf)
          break;
      }
    }

    return flush();
  }

private:
  file file_;
  io::file_output_stream stream_;
};

} // namespace sink
} // namespace vast

#endif
