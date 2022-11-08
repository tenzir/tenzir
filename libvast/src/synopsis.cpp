//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/synopsis.hpp"

#include "vast/bool_synopsis.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/logger.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/time_synopsis.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/sec.hpp>

#include <typeindex>

namespace vast {

synopsis::synopsis(vast::type x) : type_{std::move(x)} {
  // nop
}

const vast::type& synopsis::type() const {
  return type_;
}

synopsis_ptr synopsis::shrink() const {
  return nullptr;
}

bool inspect(vast::detail::legacy_deserializer& source, synopsis_ptr& ptr) {
  // Read synopsis type.
  legacy_type t;
  if (!source(t))
    return false;
  // Only nullptr has a none type.
  if (!t) {
    ptr.reset();
    return true;
  }
  // Deserialize into a new instance.
  auto new_ptr
    = factory<synopsis>::make(type::from_legacy_type(t), caf::settings{});
  if (!new_ptr || !new_ptr->deserialize(source))
    return false;
  // Change `ptr` only after successfully deserializing.
  std::swap(ptr, new_ptr);
  return true;
}

caf::expected<flatbuffers::Offset<fbs::synopsis::LegacySynopsis>>
pack(flatbuffers::FlatBufferBuilder& builder, const synopsis_ptr& synopsis,
     const qualified_record_field& fqf) {
  auto column_name = fbs::serialize_bytes(builder, fqf);
  if (!column_name)
    return column_name.error();
  auto* ptr = synopsis.get();
  if (auto* tptr = dynamic_cast<time_synopsis*>(ptr)) {
    auto min = tptr->min().time_since_epoch().count();
    auto max = tptr->max().time_since_epoch().count();
    fbs::synopsis::TimeSynopsis time_synopsis(min, max);
    fbs::synopsis::LegacySynopsisBuilder synopsis_builder(builder);
    synopsis_builder.add_qualified_record_field(*column_name);
    synopsis_builder.add_time_synopsis(&time_synopsis);
    return synopsis_builder.Finish();
  }
  if (auto* bptr = dynamic_cast<bool_synopsis*>(ptr)) {
    fbs::synopsis::BoolSynopsis bool_synopsis(bptr->any_true(),
                                              bptr->any_false());
    fbs::synopsis::LegacySynopsisBuilder synopsis_builder(builder);
    synopsis_builder.add_qualified_record_field(*column_name);
    synopsis_builder.add_bool_synopsis(&bool_synopsis);
    return synopsis_builder.Finish();
  } else {
    auto data = fbs::serialize_bytes(builder, synopsis);
    if (!data)
      return data.error();
    fbs::synopsis::LegacyOpaqueSynopsisBuilder opaque_builder(builder);
    opaque_builder.add_data(*data);
    auto opaque_synopsis = opaque_builder.Finish();
    fbs::synopsis::LegacySynopsisBuilder synopsis_builder(builder);
    synopsis_builder.add_qualified_record_field(*column_name);
    synopsis_builder.add_opaque_synopsis(opaque_synopsis);
    return synopsis_builder.Finish();
  }
  return caf::make_error(ec::logic_error, "unreachable");
}

caf::error
unpack(const fbs::synopsis::LegacySynopsis& synopsis, synopsis_ptr& ptr) {
  ptr = nullptr;
  if (auto bs = synopsis.bool_synopsis())
    ptr = std::make_unique<bool_synopsis>(bs->any_true(), bs->any_false());
  else if (auto ts = synopsis.time_synopsis())
    ptr = std::make_unique<time_synopsis>(
      vast::time{} + vast::duration{ts->start()},
      vast::time{} + vast::duration{ts->end()});
  else if (auto os = synopsis.opaque_synopsis()) {
    vast::detail::legacy_deserializer sink(as_bytes(*os->data()));
    if (!sink(ptr))
      return caf::make_error(ec::parse_error, "opaque_synopsis not "
                                              "deserializable");
  } else {
    return caf::make_error(ec::format_error, "no synopsis type");
  }
  return caf::none;
}

synopsis_ptr make_synopsis(const type& t) {
  return factory<synopsis>::make(t, caf::settings{});
}

} // namespace vast
