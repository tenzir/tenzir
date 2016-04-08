#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_BITMAP_INDEX_POLYMORPHIC_HPP
#define VAST_CONCEPT_SERIALIZABLE_VAST_BITMAP_INDEX_POLYMORPHIC_HPP

#include "vast/bitmap_index_polymorphic.hpp"
#include "vast/concept/serializable/hierarchy.hpp"
#include "vast/concept/serializable/vast/bitmap_index.hpp"
#include "vast/concept/serializable/vast/type.hpp"
#include "vast/concept/state/bitmap_index_polymorphic.hpp"

namespace vast {

template <typename Serializer, typename Bitstream>
void serialize(Serializer& sink, bitmap_index<Bitstream> const& bmi) {
  sink.template begin_instance<bitmap_index<Bitstream>>();
  if (!bmi) {
    sink << false;
    return;
  }
  sink << true;
  auto f = [&](auto& c) { polymorphic_serialize(sink, c.get()); };
  access::state<bitmap_index<Bitstream>>::call(bmi, f);
  sink.template end_instance<bitmap_index<Bitstream>>();
}

template <typename Deserializer, typename Bitstream>
void deserialize(Deserializer& source, bitmap_index<Bitstream>& bmi) {
  source.template begin_instance<bitmap_index<Bitstream>>();
  bool flag;
  source >> flag;
  if (!flag)
    return;
  detail::bitmap_index_concept<Bitstream>* bmic;
  polymorphic_deserialize(source, bmic);
  auto f = [=](auto& c) {
    c = std::unique_ptr<detail::bitmap_index_concept<Bitstream>>{bmic};
  };
  access::state<bitmap_index<Bitstream>>::call(bmi, f);
  source.template end_instance<bitmap_index<Bitstream>>();
}

} // namespace vast

#endif
