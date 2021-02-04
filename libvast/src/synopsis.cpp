/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/synopsis.hpp"

#include "vast/address_synopsis.hpp"
#include "vast/bloom_filter_synopsis.hpp"
#include "vast/bool_synopsis.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/hasher.hpp"
#include "vast/logger.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/time_synopsis.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/error.hpp>

#include <typeindex>

namespace vast {

synopsis::synopsis(vast::type x) : type_{std::move(x)} {
  // nop
}

synopsis::~synopsis() {
  // nop
}

const vast::type& synopsis::type() const {
  return type_;
}

synopsis_ptr synopsis::shrink() const {
  return nullptr;
}

caf::error inspect(caf::serializer& sink, synopsis_ptr& ptr) {
  if (!ptr) {
    static type dummy;
    return sink(dummy);
  }
  return caf::error::eval(
    [&] { return sink(ptr->type()); },
    [&] { return ptr->serialize(sink); });
}

caf::error inspect(caf::deserializer& source, synopsis_ptr& ptr) {
  // Read synopsis type.
  type t;
  if (auto err = source(t))
    return err;
  // Only nullptr has an none type.
  if (!t) {
    ptr.reset();
    return caf::none;
  }
  // Deserialize into a new instance.
  auto new_ptr = factory<synopsis>::make(std::move(t), caf::settings{});
  if (!new_ptr)
    return ec::invalid_synopsis_type;
  if (auto err = new_ptr->deserialize(source))
    return err;
  // Change `ptr` only after successfully deserializing.
  using std::swap;
  swap(ptr, new_ptr);
  return caf::none;
}

caf::expected<flatbuffers::Offset<fbs::synopsis::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, const synopsis_ptr& synopsis,
     const qualified_record_field& fqf) {
  auto column_name = fbs::serialize_bytes(builder, fqf);
  if (!column_name)
    return column_name.error();
  auto ptr = synopsis.get();
  if (auto tptr = dynamic_cast<time_synopsis*>(ptr)) {
    auto min = tptr->min().time_since_epoch().count();
    auto max = tptr->max().time_since_epoch().count();
    fbs::time_synopsis::v0 time_synopsis(min, max);
    fbs::synopsis::v0Builder synopsis_builder(builder);
    synopsis_builder.add_qualified_record_field(*column_name);
    synopsis_builder.add_time_synopsis(&time_synopsis);
    return synopsis_builder.Finish();
  } else if (auto bptr = dynamic_cast<bool_synopsis*>(ptr)) {
    fbs::bool_synopsis::v0 bool_synopsis(bptr->any_true(), bptr->any_false());
    fbs::synopsis::v0Builder synopsis_builder(builder);
    synopsis_builder.add_qualified_record_field(*column_name);
    synopsis_builder.add_bool_synopsis(&bool_synopsis);
    return synopsis_builder.Finish();
  } else {
    auto data = fbs::serialize_bytes(builder, synopsis);
    if (!data)
      return data.error();
    fbs::opaque_synopsis::v0Builder opaque_builder(builder);
    opaque_builder.add_data(*data);
    auto opaque_synopsis = opaque_builder.Finish();
    fbs::synopsis::v0Builder synopsis_builder(builder);
    synopsis_builder.add_qualified_record_field(*column_name);
    synopsis_builder.add_opaque_synopsis(opaque_synopsis);
    return synopsis_builder.Finish();
  }
  return caf::make_error(ec::logic_error, "unreachable");
}

caf::error unpack(const fbs::synopsis::v0& synopsis, synopsis_ptr& ptr) {
  ptr = nullptr;
  if (auto bs = synopsis.bool_synopsis()) {
    ptr = std::make_unique<bool_synopsis>(bs->any_true(), bs->any_false());
  } else if (auto ts = synopsis.time_synopsis()) {
    ptr = std::make_unique<time_synopsis>(
      vast::time{} + vast::duration{ts->start()},
      vast::time{} + vast::duration{ts->end()});
  } else if (auto as = synopsis.address_synopsis()) {
    switch (as->hasher()) {
      case fbs::bloom_synopsis::hasher_type::xxhash64:
        // TODO: Previously the type was serialized and deserialized along with
        // the synopsis. I think that was done just to store the the bloom
        // filter parameters as a type attribute so it should be unnecessary
        // now, but if there was more to it this may be broken now.
        ptr = std::make_unique<address_synopsis<xxhash64>>(vast::address_type{},
                                                           as);
    }
  } else if (auto os = synopsis.opaque_synopsis()) {
    caf::binary_deserializer sink(
      nullptr, reinterpret_cast<const char*>(os->data()->data()),
      os->data()->size());
    if (auto error = sink(ptr))
      return error;
  } else {
    return caf::make_error(ec::format_error, "no synopsis type");
  }
  return caf::none;
}

} // namespace vast
