#include "vast/store/segment.h"

#include <ze/event.h>
#include <ze/serialization/chunk.h>
#include <ze/serialization/container.h>
#include <ze/serialization/string.h>
#include <ze/serialization/pointer.h>
#include <ze/util/make_unique.h>
#include "vast/store/exception.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

uint32_t const basic_segment::magic = 0x2a2a2a2a;
uint8_t const basic_segment::version = 1;

uint32_t basic_segment::n_events() const
{
    return n_events_;
}

basic_segment::basic_segment()
  : version_(version)
  , n_events_(0u)
{
    start_ = ze::clock::now();
    end_ = start_;
}

basic_segment::basic_segment(basic_segment&& other)
  : object(std::move(other))
  , version_(other.version_)
  , start_(std::move(other.start_))
  , end_(std::move(other.end_))
  , events_(std::move(other.events_))
  , n_events_(other.n_events_)
{
    other.version_ = 0u;
    other.n_events_ = 0u;
}

void save(ze::serialization::oarchive& oa, basic_segment const& bs)
{
    oa << basic_segment::magic;
    oa << bs.version_;
    oa << static_cast<ze::object const&>(bs);
    oa << bs.start_;
    oa << bs.end_;
    oa << bs.events_;
    oa << bs.n_events_;
}

void load(ze::serialization::iarchive& ia, basic_segment& bs)
{
    uint32_t magic;
    ia >> magic;
    if (magic != basic_segment::magic)
        throw segment_exception("invalid segment magic");

    ia >> bs.version_;
    if (bs.version_ > basic_segment::version)
        throw segment_exception("segment version too high");

    ia >> static_cast<ze::object&>(bs);
    ia >> bs.start_;
    ia >> bs.end_;
    ia >> bs.events_;
    ia >> bs.n_events_;
}

osegment::osegment()
  : method_(ze::compression::zlib)
  , size_(0ul)
{
    chunks_.emplace_back(new ochunk(method_));
}

uint32_t osegment::put(ze::event const& event)
{
    assert(! chunks_.empty());

    ++n_events_;

    if (event.timestamp() < start_)
        start_ = event.timestamp();
    if (event.timestamp() > end_)
        end_ = event.timestamp();

    auto i = std::lower_bound(events_.begin(), events_.end(), event.name());
    if (i == events_.end())
        events_.push_back(event.name());
    else if (event.name() < *i)
        events_.insert(i, event.name());

    auto& chunk = *chunks_.back();
    chunk.put(event);
    return chunk.elements();
}

size_t osegment::size() const
{
    return size_;
}

void osegment::flush()
{
    auto size = chunks_.back()->flush();
    size_ += size;
    LOG(debug, store) << "flushed chunk" << " (" << size << "B)";
}

void osegment::push_chunk()
{
    chunks_.emplace_back(new ochunk(method_));
}

void save(ze::serialization::oarchive& oa, osegment const& segment)
{
    auto start = oa.position();
    oa << static_cast<basic_segment const&>(segment);
    auto middle = oa.position();
    oa << segment.chunks_;
    auto end = oa.position();

    auto mins = std::chrono::duration_cast<std::chrono::minutes>(
        segment.end_ - segment.start_);

    LOG(debug, store)
        << "serialized segment (#events: "
        << segment.n_events_
        << ", span: "
        << mins.count()
        << " mins, size: "
        <<  middle - start << "/"
        << end - middle << "B header/chunks)";
}

isegment::isegment(osegment&& o)
  : basic_segment(std::move(o))
{
    assert(! o.chunks_.empty());
    for (auto& chunk : o.chunks_)
        chunks_.emplace_back(std::make_unique<ichunk>(std::move(*chunk)));

    current_ = chunks_.begin();
}

void isegment::get(std::function<void(ze::event_ptr event)> f)
{
    for (auto& chunk : chunks_)
        chunk->get(f);
}

size_t isegment::get_chunk(std::function<void(ze::event_ptr event)> f)
{
    assert(current_ != chunks_.end());

    try
    {
        (**current_).get(f);
    }
    catch (ze::bad_type_exception e)
    {
        LOG(error, store) << "skipping rest of bad chunk: " << e.what();
    }
    catch (ze::serialization::exception const& e)
    {
        LOG(error, store) << "error while deserializing events: " << e.what();
        LOG(error, store)
            << "skipping rest of chunk #"
            << (current_ - chunks_.begin());
    }

    if (++current_ == chunks_.end())
    {
        current_ = chunks_.begin();
        return 0;
    }

    return chunks_.end() - current_;
}

void load(ze::serialization::iarchive& ia, isegment& segment)
{
    ia >> static_cast<basic_segment&>(segment);
    ia >> segment.chunks_;
    assert(! segment.chunks_.empty());

    // TODO: seek to next chunk on error.

    segment.current_ = segment.chunks_.begin();
}

} // namespace store
} // namespace vast
