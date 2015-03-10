#ifndef VAST_CONCEPT_SERIALIZABLE_BITSTREAM_POLYMORPHIC_H
#define VAST_CONCEPT_SERIALIZABLE_BITSTREAM_POLYMORPHIC_H

#include "vast/bitstream.h"
#include "vast/bitstream_polymorphic.h"
#include "vast/concept/serializable/hierarchy.h"
#include "vast/concept/state/bitstream_polymorphic.h"

namespace vast {

template <typename Serializer>
void serialize(Serializer& sink, bitstream const& bs)
{
  sink.template begin_instance<bitstream>();
  if (! bs)
  {
    serialize(sink, false);
    return;
  }
  serialize(sink, true);
  auto f = [&](auto& c) { polymorphic_serialize(sink, c.get()); };
  access::state<bitstream>::call(bs, f);
  sink.template end_instance<bitstream>();
}

template <typename Deserializer>
void deserialize(Deserializer& source, bitstream& bs)
{
  source.template begin_instance<bitstream>();
  bool flag;
  deserialize(source, flag);
  if (! flag)
    return;
  detail::bitstream_concept* bsc;
  polymorphic_deserialize(source, bsc);
  auto f = [=](auto& c)
  {
    c = std::unique_ptr<detail::bitstream_concept>{bsc};
  };
  access::state<bitstream>::call(bs, f);
  source.template end_instance<bitstream>();
}

} // namespace vast

#endif

