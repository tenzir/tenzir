#ifndef VAST_STORE_SEGMENT_H
#define VAST_STORE_SEGMENT_H

#define VAST_SEGMENT_VERSION    1

#include <vector>
#include <string>
#include <ze/forward.h>
#include <ze/serialization/chunk.h>
#include <ze/type/time.h>

namespace vast {
namespace store {

static uint32_t const segment_magic = 0x2a2a2a2a;

struct segment_header
{
    segment_header();

    void respect(ze::event const& event);

    uint32_t version;
    ze::time_point start;
    ze::time_point end;
    std::vector<std::string> event_names;
    uint32_t n_events;
};

void save(ze::serialization::oarchive& oa, segment_header const&);
void load(ze::serialization::oarchive& ia, segment_header&);

class isegment;

/// An output segment.
class osegment
{
    friend isegment;
    osegment(osegment const&) = delete;
    osegment& operator=(osegment) = delete;

public:
    /// Constructs an output segment.
    ///
    /// @param max_chunk_events The maximum number events per chunk.
    osegment(size_t max_chunk_events);

    /// Puts an event into the segment.
    /// @param event The event to store.
    void put(ze::event const& event);

    /// Flushes the segment to a given output stream.
    /// @param out The output stream to write the segment into.
    void flush(std::ostream& out);

    /// Retrives the size of the segment.
    /// @return The segment size in bytes (without the segment header).
    size_t size() const;

    /// Ensures that all chunks have been flushed.
    void flush();

private:
    typedef ze::serialization::ochunk<ze::event> ochunk;

    friend void save(ze::serialization::oarchive& oa, osegment const& segment);

    void flush_chunk(ochunk& chunk);

    ze::compression const method_;
    size_t const max_chunk_events_;
    size_t current_size_;

    segment_header header_;
    std::vector<std::unique_ptr<ochunk>> chunks_;
};

/// An input segment.
class isegment
{
    isegment(isegment const&) = delete;
    isegment& operator=(isegment) = delete;

public:
    /// Constructs an empty input segment that should be serialized into.
    isegment() = default;

    /// Constructs an input segment from on output segment.
    /// @param o The output segment to steal the chunks from.
    isegment(osegment&& o);

    void get(std::function<void(ze::event_ptr&& event)> f);

private:
    typedef ze::serialization::ichunk<ze::event_ptr> ichunk;

    friend void load(ze::serialization::iarchive& ia, isegment& segment);

    segment_header header_;
    std::vector<std::unique_ptr<ichunk>> chunks_;
};

} // namespace store
} // namespace vast

#endif
