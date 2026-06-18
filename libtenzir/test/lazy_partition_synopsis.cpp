//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fbs/partition_synopsis.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

#include <flatbuffers/flatbuffers.h>

#include <map>
#include <string>

using namespace tenzir;

namespace {

// Builds a partition synopsis with one inline (time) and one opaque (int64
// min/max) field synopsis, packs it, and unpacks it again with the given lazy
// sketch threshold.
auto roundtrip(size_t lazy_sketch_threshold) -> partition_synopsis {
  factory<synopsis>::initialize();
  partition_synopsis ps;
  ps.schema = type{record_type{{"ts", time_type{}}, {"n", int64_type{}}}};
  const auto ts_field = qualified_record_field{"test", "ts", type{time_type{}}};
  const auto n_field = qualified_record_field{"test", "n", type{int64_type{}}};
  // A time synopsis is serialized inline (never deferred); an int64 min/max
  // synopsis is serialized as an opaque blob (deferred above the threshold).
  ps.field_synopses_[ts_field]
    = factory<synopsis>::make(type{time_type{}}, caf::settings{});
  ps.field_synopses_[n_field]
    = factory<synopsis>::make(type{int64_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(ps.field_synopses_[ts_field], nullptr);
  REQUIRE_NOT_EQUAL(ps.field_synopses_[n_field], nullptr);
  flatbuffers::FlatBufferBuilder builder;
  auto offset = pack(builder, ps);
  REQUIRE(static_cast<bool>(offset));
  fbs::PartitionSynopsisBuilder ps_builder(builder);
  ps_builder.add_partition_synopsis_type(
    fbs::partition_synopsis::PartitionSynopsis::legacy);
  ps_builder.add_partition_synopsis(offset->Union());
  auto root = ps_builder.Finish();
  fbs::FinishPartitionSynopsisBuffer(builder, root);
  const auto* fb = fbs::GetPartitionSynopsis(builder.GetBufferPointer());
  REQUIRE(fb != nullptr);
  const auto* legacy = fb->partition_synopsis_as_legacy();
  REQUIRE(legacy != nullptr);
  partition_synopsis out;
  REQUIRE_EQUAL(unpack(*legacy, out, lazy_sketch_threshold), caf::none);
  return out;
}

// Maps each field name to whether its synopsis is null (deferred).
auto field_is_null(const partition_synopsis& ps)
  -> std::map<std::string, bool> {
  std::map<std::string, bool> result;
  for (const auto& [field, synopsis] : ps.field_synopses_) {
    result[std::string{field.field_name()}] = synopsis == nullptr;
  }
  return result;
}

} // namespace

TEST("lazy sketch threshold of zero loads every synopsis") {
  const auto ps = roundtrip(0);
  const auto fields = field_is_null(ps);
  REQUIRE_EQUAL(fields.size(), 2u);
  CHECK(not fields.at("ts"));
  CHECK(not fields.at("n"));
}

TEST("lazy sketch threshold defers opaque synopses but keeps the field keys") {
  // A threshold of 8 bytes is below the serialized size of the int64 min/max
  // synopsis, so it is deferred, while the inline time synopsis is unaffected.
  const auto ps = roundtrip(8);
  const auto fields = field_is_null(ps);
  // Crucially, both field keys are still present: the catalog relies on this to
  // treat predicates on the deferred field as candidates rather than silently
  // dropping the partition (which would be a false negative).
  REQUIRE_EQUAL(fields.size(), 2u);
  CHECK(not fields.at("ts"));
  CHECK(fields.at("n"));
}
