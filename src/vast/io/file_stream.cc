#include "vast/io/file_stream.h"

namespace vast {
namespace io {

file_input_device::file_input_device(path const& filename)
  : file_(filename)
{
  file_.open(file::read_only);
}

file_input_device::file_input_device(file::native_type handle,
                                     bool close_behavior)
  : file_(handle, close_behavior)
{
}

bool file_input_device::read(void* data, size_t bytes, size_t* got)
{
  return file_.read(data, bytes, got);
}

bool file_input_device::skip(size_t bytes, size_t *skipped)
{
  if (file_.seek(bytes))
  {
    if (skipped != nullptr)
      *skipped = bytes;
    return true;
  }
  return input_device::skip(bytes, skipped);
}


file_input_stream::file_input_stream(path const& filename, size_t block_size)
  : buffer_(filename),
    buffered_stream_(buffer_, block_size)
{
}

file_input_stream::file_input_stream(file::native_type handle, bool close_behavior,
                                     size_t block_size)
  : buffer_(handle, close_behavior),
    buffered_stream_(buffer_, block_size)
{
}

bool file_input_stream::next(void const** data, size_t* size)
{
  return buffered_stream_.next(data, size);
}

void file_input_stream::rewind(size_t bytes)
{
  buffered_stream_.rewind(bytes);
}

bool file_input_stream::skip(size_t bytes)
{
  return buffered_stream_.skip(bytes);
}

uint64_t file_input_stream::bytes() const
{
  return buffered_stream_.bytes();
}


file_output_device::file_output_device(path const& filename)
  : file_(filename)
{
  file_.open(file::write_only);
}

bool file_output_device::write(void const* data, size_t bytes, size_t* put)
{
  return file_.write(data, bytes, put);
}

file_output_device::file_output_device(file::native_type handle,
                                       bool close_behavior)
  : file_(handle, close_behavior)
{
}

file_output_stream::file_output_stream(path const& filename, size_t block_size)
  : buffer_(filename),
    buffered_stream_(buffer_, block_size)
{
}

file_output_stream::file_output_stream(file::native_type handle, bool close_behavior,
                                     size_t block_size)
  : buffer_(handle, close_behavior),
    buffered_stream_(buffer_, block_size)
{
}

file_output_stream::~file_output_stream()
{
  flush();
}

bool file_output_stream::next(void** data, size_t* size)
{
  return buffered_stream_.next(data, size);
}

void file_output_stream::rewind(size_t bytes)
{
  buffered_stream_.rewind(bytes);
}

bool file_output_stream::flush()
{
  return buffered_stream_.flush();
}

uint64_t file_output_stream::bytes() const
{
  return buffered_stream_.bytes();
}

} // namespace io
} // namespace vast
