#include <algorithm>

#include <caf/all.hpp>

#include "vast/actor/atoms.h"
#include "vast/io/actor_stream.h"

using namespace caf;

namespace vast {
namespace io {

actor_input_stream::actor_input_stream(actor source,
                                       std::chrono::milliseconds timeout)
  : source_{std::move(source)},
    timeout_{timeout}
{
  assert(max_inflight_ > 0);
}

bool actor_input_stream::next(void const** data, size_t* size)
{
  // Check if we can satisy the request with existing data.
  if (rewind_bytes_ > 0)
  {
    *data = data_.front().data() + data_.front().size() - rewind_bytes_;
    *size = rewind_bytes_;
    rewind_bytes_ = 0;
    return true;
  }
  if (! data_.empty())
    data_.pop();
  else if (done_)
    return false;
  // Whenever we pop a chunk, we try to grab a new one.
  if (! done_)
  {
    assert(max_inflight_ > data_.size());
    for (auto i = data_.size(); i < max_inflight_; ++i)
      self_->send(source_, get_atom::value);
    auto got_timeout = false;
    self_->do_receive(
        [&](std::vector<uint8_t>& data) { data_.push(std::move(data)); },
        [&](done_atom) { done_ = true; },
        after(data_.empty() ? timeout_ : std::chrono::seconds(0)) >> [&]
        {
          got_timeout = true;
        }
      ).until([&] { return got_timeout; });
  }
  if (! data_.empty())
  {
    *data = data_.front().data();
    *size = data_.front().size();
    rewind_bytes_ = 0;
    position_ += *size;
    return true;
  }
  return false;
}

void actor_input_stream::rewind(size_t bytes)
{
  assert(! data_.empty());
  if (rewind_bytes_ + bytes <= data_.front().size())
    rewind_bytes_ += bytes;
  else
    rewind_bytes_ = data_.front().size();
}

bool actor_input_stream::skip(size_t bytes)
{
  if (rewind_bytes_ > 0)
  {
    assert(! data_.empty());
    if (bytes <= rewind_bytes_)
    {
      rewind_bytes_ -= bytes;
      return true;
    }
    bytes -= rewind_bytes_;
    rewind_bytes_ = 0;
  }
  void const* data;
  size_t size;
  while (bytes > 0)
  {
    if (! next(&data, &size))
      return false;
    if (bytes <= size)
    {
      rewind_bytes_ = size - bytes;
      break;
    }
    bytes -= size;
  }
  return true;
}

uint64_t actor_input_stream::bytes() const
{
  return position_ - rewind_bytes_;
}


actor_output_device::actor_output_device(actor sink)
  : sink_{std::move(sink)}
{
}

bool actor_output_device::write(void const* data, size_t size, size_t* put)
{
  auto v = std::vector<uint8_t>(size);
  auto p = reinterpret_cast<uint8_t const*>(data);
  std::copy(p, p + size, v.data());
  anon_send(sink_, std::move(v));
  if (put != nullptr)
    *put = size;
  return true;
}

actor_output_stream::actor_output_stream(actor sink, size_t block_size)
  : device_{std::move(sink)},
    buffered_stream_{device_, block_size}
{
}

bool actor_output_stream::flush()
{
  return buffered_stream_.flush();
}

bool actor_output_stream::next(void** data, size_t* size)
{
  return buffered_stream_.next(data, size);
}

void actor_output_stream::rewind(size_t bytes)
{
  buffered_stream_.rewind(bytes);
}

uint64_t actor_output_stream::bytes() const
{
  return buffered_stream_.bytes();
}

} // namespace io
} // namespace vast
