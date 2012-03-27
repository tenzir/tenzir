#include "vast/store/segment.h"

#include <ze/event.h>
#include <ze/serialization/string.h>
#include "vast/util/logger.h"

namespace vast {
namespace store {

segment_header::segment_header()
  : magic(VAST_SEGMENT_MAGIC)
  , version(VAST_SEGMENT_VERSION)
  , n_events(0u)
{
}

void segment_header::respect(ze::event const& event)
{
    ++n_events;

    if (event.timestamp() < start)
        start = event.timestamp();
    if (event.timestamp() > end)
        end = event.timestamp();

    auto i = std::lower_bound(event_names.begin(),
                              event_names.end(),
                              event.name());

    if (i == event_names.end())
        event_names.push_back(event.name());
    else if (event.name() < *i)
        event_names.insert(i, event.name());
}

void save(ze::serialization::oarchive& oa, segment_header const& header)
{
    oa << header.magic;
    oa << header.version;
    oa << header.start;
    oa << header.end;
    oa << header.event_names;
    oa << header.n_events;
}

osegment::osegment(size_t max_chunk_size)
  : method_(ze::compression::zlib)
  , max_chunk_size_(max_chunk_size)
  , current_size_(0ul)
{
    chunks_.emplace_back(new ochunk(method_));
}

void osegment::put(ze::event const& event)
{
    header_.respect(event);
    auto& chunk = *chunks_.back();
    chunk.put(event);
    if (chunk.buffer().size() >= max_chunk_size_)
    {
        chunk.flush();
        current_size_ += sizeof(ze::serialization::chunk_header);
        current_size_ += chunk.buffer().size();

        LOG(debug, store)
            << "new segment chunk (old chunk size: "
            << chunk.buffer().size() << ')';

        chunks_.emplace_back(new ochunk(method_));
    }
}

size_t osegment::size() const
{
    return current_size_;
}

void osegment::flush(std::ostream& out)
{
    LOG(debug, store) << "flushing segment";

    // Only the last chunk has not yet been flushed.
    chunks_.back()->flush();

    ze::serialization::oarchive oa(out);
    oa << header_;
    oa << chunks_.size();
    for (auto& chunk : chunks_)
        oa << *chunk;
}

} // namespace store
} // namespace vast
