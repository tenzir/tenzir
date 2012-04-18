#ifndef VAST_STORE_SEGMENT_H
#define VAST_STORE_SEGMENT_H

#include <memory>
#include <vector>
#include <string>
#include <ze/forward.h>
#include <ze/intrusive.h>
#include <ze/object.h>
#include <ze/type/time.h>

namespace vast {
namespace store {

class basic_segment : public ze::object
                    , ze::intrusive_base<basic_segment>
{
    basic_segment(basic_segment const&) = delete;
    basic_segment& operator=(basic_segment) = delete;

public:
    uint32_t n_events() const;

protected:
    basic_segment();
    basic_segment(basic_segment&& other);

    uint32_t version_;
    ze::time_point start_;
    ze::time_point end_;
    std::vector<std::string> events_;
    uint32_t n_events_;

private:
    friend void save(ze::serialization::oarchive& oa, basic_segment const& bs);
    friend void load(ze::serialization::iarchive& ia, basic_segment& bs);

    // FIXME: gives a linker error w/ gcc 4.7, although this is legal in C++11.
    //static uint32_t const magic = 0x2a2a2a2a;
    //static uint8_t const version = 1;
    static uint32_t const magic;
    static uint8_t const version;
};

class isegment;

/// An output segment.
class osegment : public basic_segment
{
    friend isegment;
    osegment(osegment const&) = delete;
    osegment& operator=(osegment) = delete;

public:
    /// Constructs an output segment.
    osegment();

    /// Puts an event into the segment.
    /// @param event The event to store.
    /// @return The number of events in the current chunk
    uint32_t put(ze::event const& event);

    /// Retrieves the size of the segment.
    /// @return The segment size in bytes (without the segment header).
    size_t size() const;

    /// Flushes the currently active chunk.
    void flush();

    /// Creates a new chunk at the end of the segment.
    void push_chunk();

private:
    typedef ze::serialization::ochunk<ze::event> ochunk;

    friend void save(ze::serialization::oarchive& oa, osegment const& segment);

    ze::compression const method_;
    size_t size_;

    std::vector<std::unique_ptr<ochunk>> chunks_;
};

/// An input segment.
class isegment : public basic_segment
{
    isegment(isegment const&) = delete;
    isegment& operator=(isegment) = delete;

public:
    /// Constructs an empty input segment that should be serialized into.
    isegment() = default;

    /// Constructs an input segment from an output segment.
    /// @param o The output segment to steal the chunks from.
    isegment(osegment&& o);

    /// Invokes a function on each event of chunks.
    /// @param f The function to invoke on each event.
    void get(std::function<void(ze::event_ptr event)> f);

    /// Invokes a function on each event from the current chunk and switch to
    /// the next chunk afterwards.
    /// @param f The function to invoke on each event.
    /// @return The number of chunks left in the segment.
    size_t get_chunk(std::function<void(ze::event_ptr event)> f);

private:
    typedef ze::serialization::ichunk<ze::event_ptr> ichunk;

    friend void load(ze::serialization::iarchive& ia, isegment& segment);

    std::vector<std::unique_ptr<ichunk>> chunks_;
    std::vector<std::unique_ptr<ichunk>>::const_iterator current_;
};

} // namespace store
} // namespace vast

#endif
