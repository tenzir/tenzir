#include "vast/search_result.h"

#include "vast/serialization.h"

namespace vast {

search_result::search_result(bitstream result, bitstream coverage)
  : hits_{std::move(result)},
    coverage_{std::move(coverage)}
{
}

search_result::operator bool() const
{
  return hits_ && coverage_;
}

search_result& search_result::operator&=(search_result const& other)
{
  assert(other);
  if (! *this)
  {
    hits_ = other.hits_;
    coverage_ = other.coverage_;
  }
  else
  {
    hits_ &= other.hits_;
    coverage_ &= other.coverage_;
  }
  return *this;
}

search_result& search_result::operator|=(search_result const& other)
{
  assert(other);
  if (! *this)
  {
    hits_ = other.hits_;
    coverage_ = other.coverage_;
  }
  else
  {
    hits_ |= other.hits_;
    coverage_ |= other.coverage_;
  }
  return *this;
}

bitstream const& search_result::hits() const
{
  return hits_;
}

bitstream const& search_result::coverage() const
{
  return coverage_;
}

void search_result::serialize(serializer& sink) const
{
  sink << hits_ << coverage_;
}

void search_result::deserialize(deserializer& source)
{
  source >> hits_ >> coverage_;
}

bool operator==(search_result const& x, search_result const& y)
{
  return x.hits_ && y.hits_ && x.hits_ == y.hits_
      && x.coverage_ && y.coverage_ && x.coverage_ == y.coverage_;
}

} // namespace vast
