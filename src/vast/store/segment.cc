#include "vast/store/segment.h"

#include <ze/event.h>
#include <ze/serialization/container.h>
#include <ze/serialization/string.h>
#include <ze/serialization/pointer.h>
#include "vast/store/exception.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

segment_header::segment_header()
  : magic(VAST_SEGMENT_MAGIC)
  , version(VAST_SEGMENT_VERSION)
  , n_events(0u)
{
    start = ze::clock::now();
    end = start;
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

void load(ze::serialization::iarchive& ia, segment_header& header)
{
    ia >> header.magic;
    ia >> header.version;
    ia >> header.start;
    ia >> header.end;
    ia >> header.event_names;
    ia >> header.n_events;
}

osegment::osegment(size_t max_chunk_events)
  : method_(ze::compression::zlib)
  , max_chunk_events_(max_chunk_events)
  , current_size_(0u)
{
    chunks_.emplace_back(new ochunk(method_));

    LOG(debug, store)
        << "creating segment with " << max_chunk_events_ << " events/chunk";
}

void osegment::put(ze::event const& event)
{
    assert(! chunks_.empty());

    header_.respect(event);
    auto& chunk = *chunks_.back();
    chunk.put(event);

    if (chunk.elements() < max_chunk_events_)
        return;

    flush_chunk(chunk);
    chunks_.emplace_back(new ochunk(method_));
}

size_t osegment::size() const
{
    return current_size_;
}

void osegment::flush(std::ostream& out)
{
    // Only the last chunk has not yet been flushed.
    flush_chunk(*chunks_.back());

    ze::serialization::oarchive oa(out);

    auto start = out.tellp();
    oa << header_;
    auto middle = out.tellp();
    oa << chunks_;
    auto end = out.tellp();

    LOG(debug, store)
        << "flushed segment "
        << "(header: " << middle - start << " bytes, "
        << "chunks: " << end - middle << " bytes)";

    clear();
}

void osegment::flush_chunk(ochunk& chunk)
{
    auto size = chunk.flush();
    current_size_ += size;
    LOG(debug, store)
        << "flushed chunk" << " (" << size << " bytes)";
}

void osegment::clear()
{
    chunks_.clear();
    chunks_.emplace_back(new ochunk(method_));
    header_ = segment_header();
    current_size_ = 0u;
}


isegment::isegment(std::istream& in)
  : istream_(in)
{
    ze::serialization::iarchive ia(istream_);
    ia >> header_;

    if (header_.magic != VAST_SEGMENT_MAGIC)
        throw segment_exception("invalid segment magic");

    if (header_.version > VAST_SEGMENT_VERSION)
        throw segment_exception("cannot handle segment version");
}

void isegment::get(std::function<void(ze::event_ptr&& event)> f)
{
    std::vector<std::unique_ptr<ichunk>> chunks;
    ze::serialization::iarchive ia(istream_);
    ia >> chunks;

    for (auto& chunk : chunks)
        while (! chunk->empty())
        {
            ze::event_ptr event = new ze::event;
            chunk->get(*event);
            f(std::move(event));
        }
}

} // namespace store
} // namespace vast
