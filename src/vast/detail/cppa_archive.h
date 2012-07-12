#ifndef VAST_DETAIL_CPPA_ARCHIVE_H
#define VAST_DETAIL_CPPA_ARCHIVE_H

#include <cppa/deserializer.hpp>
#include <cppa/serializer.hpp>
#include <ze/serialization/archive.h>

namespace vast {
namespace detail {

class cppa_oarchive : public ze::serialization::oarchive<cppa_oarchive>
{
public:
  cppa_oarchive(cppa::serializer* sink, std::string const& name);
  ~cppa_oarchive();
  void write_raw(void const* x, size_t n);

private:
  cppa::serializer* sink_;
};

class cppa_iarchive : public ze::serialization::iarchive<cppa_iarchive>
{
public:
  cppa_iarchive(cppa::deserializer* source, std::string const& name);
  ~cppa_iarchive();
  void read_raw(void* x, size_t n);

private:
  cppa::deserializer* source_;
};

} // namespace detail
} // namespace vast

#endif
