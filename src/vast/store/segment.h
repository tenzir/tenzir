#ifndef VAST_STORE_SEGMENT_H
#define VAST_STORE_SEGMENT_H

#define VAST_SEGMENT_MAGIC      0x2a2a2a2a
#define VAST_SEGMENT_VERSION    1

#include <vector>
#include <string>
#include <ze/forward.h>
#include <ze/serialization/chunk.h>
#include <ze/type/time.h>

namespace vast {
namespace store {

struct segment_header
{
    segment_header();

    void respect(ze::event const& event);

    uint32_t magic;
    uint32_t version;
    ze::time_point start;
    ze::time_point end;
    std::vector<std::string> event_names;
    uint32_t n_events;
};

void save(ze::serialization::oarchive& oa, segment_header const&);
void load(ze::serialization::oarchive& ia, segment_header&);

/// An output segment.
class osegment
{
public:
    /// Constructs an output segment.
    ///
    /// @param max_chunk_size The maximum chunk size in bytes. This is just an
    /// lower bound on the actual buffer size because it is impossible to know
    /// the exact chunk size before flushing the stream.
    osegment(size_t max_chunk_size);

    void put(ze::event const& event);

    /// Flushes the segment to a given output stream.
    /// @param out The output stream to write the segment into.
    void flush(std::ostream& out);

    /// Retrives the size of the segment.
    /// @return The segment size in bytes.
    size_t size() const;

private:
    typedef ze::serialization::ochunk<ze::event> ochunk;

    ze::compression const method_;
    size_t const max_chunk_size_;
    size_t current_size_;
    segment_header header_;
    std::vector<std::unique_ptr<ochunk>> chunks_;
};

class isegment
{
public:
    void put(ze::event const& event);

private:
    typedef ze::serialization::ichunk<ze::event> ichunk;

    std::vector<ichunk> chunks_;
};

} // namespace store
} // namespace vast

#endif
