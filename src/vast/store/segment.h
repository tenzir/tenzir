#ifndef VAST_STORE_SEGMENT_H
#define VAST_STORE_SEGMENT_H

#include <memory>
#include <vector>
#include <string>
#include <ze/forward.h>
#include <ze/object.h>
#include <ze/compression.h>
#include <ze/serialization.h>
#include <ze/type/time.h>
#include <vast/store/exception.h>

// TODO: make segments first-class libcppa citizens.

namespace vast {
namespace store {

class segment : public ze::object
{
  typedef ze::serialization::chunk<ze::event> chunk_type;

  struct chunk_creator
  {
    chunk_creator(segment& segment)
    {
      segment.chunks_.emplace_back();
    }
  };

public:
  class writer : chunk_creator
  {
    writer(writer const&) = delete;
    writer& operator=(writer) = delete;

  public:
    /// Creates a new chunk at the end of the segment for writing.
    /// @param s The segment to write to.
    writer(segment& s);

    /// Append a new chunk to the chunk vector and use it for subsequent write
    /// operations.
    void new_chunk();

    /// Serializes an event into the segment.
    /// @param event The event to store.
    /// @return The number of events in the current chunk
    uint32_t operator<<(ze::event const& event);

    /// Retrieves the number of bytes processed.
    /// @return The number of bytes written to the output archive of the
    /// underlying chunk.
    size_t bytes() const;

  private:
    size_t bytes_ = 0;
    segment& segment_;
    chunk_type::putter putter_;
  };

  class reader
  {
    reader(reader const&) = delete;
    reader& operator=(reader) = delete;

  public:
    /// Creates a reader for a specific segment chunk.
    ///
    /// @param s The segment to read from.
    ///
    /// @param i The chunk index, must be in *[0, n)* where *n* is the
    /// number of chunks in `s`.
    reader(chunk_type const& chunk);

    /// Deserializes an event into the segment.
    /// @param event The event to deserialize into.
    /// @return The number of events left for extraction in the current chunk.
    uint32_t operator>>(ze::event & event);

    /// Invokes a callback on each deserialized event.
    /// @param A callback taking the new event as argument.
    /// @return The number of bytes processed.
    size_t read(std::function<void(ze::event)> f);

    /// Retrieves the number of bytes processed.
    /// @return The number of bytes written from the input archive of the
    /// underlying chunk.
    size_t bytes() const;

  private:
    size_t bytes_ = 0;
    chunk_type::getter getter_;
  };

  segment(ze::compression method = ze::compression::none);

  /// Creates a reader proxy to read a given chunk.
  ///
  /// @param i The chunk index, must be in *[0, n)* where *n* is the
  /// number of chunks in `s`.
  reader read(size_t i) const;
  writer write();


  uint32_t events() const;

  /// Retrieves the number of chunks.
  /// @return The number of chunks in the segment..
  size_t chunks() const;

  /// Retrieves the number of bytes the segment occupies.
  /// @return The number of bytes the segment occupies.
  size_t bytes() const;

private:
  template <typename Archive>
  friend void save(Archive& oa, segment const& s)
  {
    oa << segment::magic;
    oa << s.version_;
    oa << static_cast<ze::object const&>(s);
    oa << s.compression_;
    oa << s.start_;
    oa << s.end_;
    oa << s.event_names_;
    oa << s.events_;
    oa << s.chunks_;

    // FIXME: bring back in as compiled function.
    //LOG(verbose, store)
    //  << "serialized segment (#events: "
    //  << segment.events_
    //  << ", span: "
    //  << mins.count()
    //  << " mins, size: "
    //  <<  middle - start << "/"
    //  << end - middle << "B header/chunks)";
  }

  template <typename Archive>
  void load(Archive& ia, segment& s)
  {
    uint32_t magic;
    ia >> magic;
    if (magic != segment::magic)
      throw segment_exception("invalid segment magic");

    ia >> s.version_;
    if (s.version_ > segment::version)
      throw segment_exception("segment version too high");

    ia >> static_cast<ze::object&>(s);
    ia >> s.compression_;
    ia >> s.start_;
    ia >> s.end_;
    ia >> s.event_names_;
    ia >> s.events_;
    ia >> s.chunks_;
  }

  static uint32_t const magic = 0x2a2a2a2a;
  static uint8_t const version = 1;

  uint32_t version_;
  ze::compression compression_;
  ze::time_point start_;
  ze::time_point end_;
  std::vector<std::string> event_names_;
  uint32_t events_;
  std::vector<chunk_type> chunks_;
};

} // namespace store
} // namespace vast

#endif
