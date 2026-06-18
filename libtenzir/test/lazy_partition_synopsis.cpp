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
#include "tenzir/series.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

#include <arrow/builder.h>
#include <caf/settings.hpp>
#include <flatbuffers/flatbuffers.h>

#include <map>
#include <string>

using namespace tenzir;

namespace {

auto make_string_series(std::vector<std::string> values) -> series {
  auto builder = arrow::StringBuilder{};
  for (const auto& value : values) {
    const auto status = builder.Append(value);
    TENZIR_ASSERT(status.ok());
  }
  auto result = builder.Finish();
  TENZIR_ASSERT(result.ok());
  return series{type{string_type{}}, std::move(*result)};
}

// Builds a partition synopsis with a Bloom-filter (string) field, an opaque
// min/max (int64) field, and an inline (time) field, packs it, and unpacks it
// again with the given lazy sketch threshold.
auto roundtrip(size_t lazy_sketch_threshold) -> partition_synopsis {
  factory<synopsis>::initialize();
  partition_synopsis ps;
  ps.schema = type{record_type{
    {"msg", string_type{}}, {"n", int64_type{}}, {"ts", time_type{}}}};
  const auto msg_field
    = qualified_record_field{"test", "msg", type{string_type{}}};
  const auto n_field = qualified_record_field{"test", "n", type{int64_type{}}};
  const auto ts_field = qualified_record_field{"test", "ts", type{time_type{}}};
  // The string synopsis is a Bloom filter (opaque, deferrable); the int64
  // synopsis is an opaque min/max; the time synopsis is encoded inline.
  auto string_opts = caf::settings{};
  string_opts["buffer-input-data"] = true;
  string_opts["max-partition-size"] = uint64_t{1024};
  string_opts["string-synopsis-fp-rate"] = 0.01;
  auto bloom = factory<synopsis>::make(type{string_type{}}, string_opts);
  REQUIRE_NOT_EQUAL(bloom, nullptr);
  bloom->add(make_string_series({"alpha", "beta", "gamma"}));
  ps.field_synopses_[msg_field] = std::move(bloom);
  ps.field_synopses_[n_field]
    = factory<synopsis>::make(type{int64_type{}}, caf::settings{});
  ps.field_synopses_[ts_field]
    = factory<synopsis>::make(type{time_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(ps.field_synopses_[n_field], nullptr);
  REQUIRE_NOT_EQUAL(ps.field_synopses_[ts_field], nullptr);
  // Materialize buffered synopses, as the partition persist path does before
  // packing.
  ps.shrink();
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
  REQUIRE_EQUAL(fields.size(), 3u);
  CHECK(not fields.at("msg"));
  CHECK(not fields.at("n"));
  CHECK(not fields.at("ts"));
}

TEST("lazy sketch threshold only defers bloom filters") {
  // A small threshold defers the Bloom-filter sketch but must never defer the
  // numeric min/max or the inline time synopsis, regardless of their size --
  // otherwise range pruning would silently break. All field keys must remain
  // present so the catalog never drops a partition (a false negative).
  const auto ps = roundtrip(8);
  const auto fields = field_is_null(ps);
  REQUIRE_EQUAL(fields.size(), 3u);
  CHECK(fields.at("msg"));    // string Bloom filter: deferred
  CHECK(not fields.at("n"));  // int64 min/max: always loaded
  CHECK(not fields.at("ts")); // time: always loaded
}
