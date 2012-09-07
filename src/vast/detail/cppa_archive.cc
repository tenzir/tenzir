#include <vast/detail/cppa_archive.h>

namespace vast {
namespace detail {

using namespace cppa;

cppa_oarchive::cppa_oarchive(serializer* sink, std::string const& name)
  : sink_(sink)
{
  sink_->begin_object(name);
}

cppa_oarchive::~cppa_oarchive()
{
  sink_->end_object();
}

void cppa_oarchive::write_raw(void const* x, size_t n)
{
  sink_->write_raw(n, x);
}


cppa_iarchive::cppa_iarchive(deserializer* source, std::string const& name)
  : source_(source)
{
  source_->begin_object(name);
}

cppa_iarchive::~cppa_iarchive()
{
  source_->end_object();
}

void cppa_iarchive::read_raw(void* x, size_t n)
{
  source_->read_raw(n, x);
}

} // namespace detail
} // namespace vast
