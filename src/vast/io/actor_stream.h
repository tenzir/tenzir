#ifndef VAST_IO_ACTOR_STREAM_H
#define VAST_IO_ACTOR_STREAM_H

#include <queue>
#include <vector>

#include <caf/actor.hpp>
#include <caf/scoped_actor.hpp>

#include "vast/io/device.h"
#include "vast/io/buffered_stream.h"

namespace vast {
namespace io {

/// An actor input stream
class actor_input_stream : public input_stream {
public:
  /// Constructs an actor input stream from an actor.
  /// @param source The actor to poll for new data chunks.
  actor_input_stream(caf::actor source, std::chrono::milliseconds timeout);

  actor_input_stream(actor_input_stream&&) = default;
  actor_input_stream& operator=(actor_input_stream&&) = default;

  virtual bool next(void const** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual bool skip(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  bool done_ = false;
  uint64_t const max_inflight_ = 2;
  caf::scoped_actor self_;
  caf::actor source_;
  std::chrono::milliseconds timeout_;
  std::queue<std::vector<uint8_t>> data_;
  size_t rewind_bytes_ = 0;
  uint64_t position_ = 0;
};

/// An output device which sends its data as byte vectors to a sink actor.
class actor_output_device : public output_device {
public:
  /// Constructs an actor output device.
  /// @param sink The actor receiving the written data.
  actor_output_device(caf::actor sink);

  bool write(void const* data, size_t size, size_t* put) override;

private:
  caf::actor sink_;
};

/// An output device which sends its buffered data as byte vectors to a sink
/// actor upon flushing.
class actor_output_stream : public output_stream {
public:
  /// Constructs an actor output stream.
  /// @param sink The sink actor receiving byte vectors.
  actor_output_stream(caf::actor sink, size_t block_size = 0);

  bool next(void** data, size_t* size) override;
  void rewind(size_t bytes) override;
  bool flush() override;
  uint64_t bytes() const override;

private:
  actor_output_device device_;
  buffered_output_stream buffered_stream_;
};

} // namespace io
} // namespace vast

#endif
