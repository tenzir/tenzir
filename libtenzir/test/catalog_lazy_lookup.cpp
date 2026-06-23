//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/fbs/partition_synopsis.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/series.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"
#include "tenzir/uuid.hpp"

#include <arrow/builder.h>
#include <caf/make_copy_on_write.hpp>
#include <flatbuffers/flatbuffers.h>

#include <filesystem>

using namespace tenzir;

namespace {

auto make_string_series(std::vector<std::string> values) -> series {
  auto builder = arrow::StringBuilder{};
  for (const auto& value : values) {
    TENZIR_ASSERT(builder.Append(value).ok());
  }
  auto array = builder.Finish();
  TENZIR_ASSERT(array.ok());
  return series{type{string_type{}}, std::move(*array)};
}

// Serializes a partition synopsis (with its Bloom-filter sketch) to a `.mdx`
// file and returns the path.
auto write_mdx(const partition_synopsis& ps) -> std::filesystem::path {
  flatbuffers::FlatBufferBuilder builder;
  auto offset = pack(builder, ps);
  TENZIR_ASSERT(offset);
  fbs::PartitionSynopsisBuilder ps_builder(builder);
  ps_builder.add_partition_synopsis_type(
    fbs::partition_synopsis::PartitionSynopsis::legacy);
  ps_builder.add_partition_synopsis(offset->Union());
  fbs::FinishPartitionSynopsisBuffer(builder, ps_builder.Finish());
  auto chunk = fbs::release(builder);
  auto path = std::filesystem::temp_directory_path()
              / fmt::format("tnz-test-{}.mdx", uuid::random());
  auto error = io::save(path, std::span{chunk->data(), chunk->size()});
  TENZIR_ASSERT(not error);
  return path;
}

// Reads a `.mdx` back, unpacking with the given laziness.
auto read_mdx(const std::filesystem::path& path, bool lazy)
  -> partition_synopsis_ptr {
  auto chunk = chunk::mmap(path);
  TENZIR_ASSERT(chunk);
  auto fb = flatbuffer<fbs::PartitionSynopsis>::make(std::move(*chunk));
  TENZIR_ASSERT(fb);
  auto ps = caf::make_copy_on_write<partition_synopsis>();
  auto error
    = unpack(*(*fb)->partition_synopsis_as_legacy(), ps.unshared(), lazy);
  TENZIR_ASSERT(not error);
  return ps;
}

// Builds a resident (Bloom-filter-deferred) synopsis whose sketch lives in a
// `.mdx` on disk, plus that schema/id, for use across the tests below.
struct fixture {
  type schema = type{"test", record_type{{"msg", string_type{}}}};
  qualified_record_field msg
    = qualified_record_field{"test", "msg", type{string_type{}}};
  uuid id = uuid::random();
  std::filesystem::path mdx;
  partition_synopsis_ptr resident;

  fixture() {
    factory<synopsis>::initialize();
    auto full = caf::make_copy_on_write<partition_synopsis>();
    full.unshared().schema = schema;
    full.unshared().events = 1;
    auto opts = caf::settings{};
    opts["buffer-input-data"] = true;
    opts["max-partition-size"] = uint64_t{1024};
    // A very low false-positive rate makes the "absent" lookup deterministic.
    opts["string-synopsis-fp-rate"] = 0.000001;
    auto bloom = factory<synopsis>::make(type{string_type{}}, opts);
    TENZIR_ASSERT(bloom);
    bloom->add(make_string_series({"hello"}));
    full.unshared().field_synopses_[msg] = std::move(bloom);
    full.unshared().shrink();
    mdx = write_mdx(*full);
    resident = read_mdx(mdx, /*lazy=*/true);
    resident.unshared().sketches_file
      = {.url = fmt::format("file://{}", mdx.string()), .size = 0};
  }

  ~fixture() {
    std::error_code ec{};
    std::filesystem::remove(mdx, ec);
  }
};

} // namespace

TEST("catalog loads deferred bloom filters on demand to prune") {
  auto f = fixture{};
  // The resident synopsis has a deferred (null) Bloom-filter sketch.
  REQUIRE_EQUAL(f.resident->field_synopses_.at(f.msg), nullptr);
  auto state = catalog_state{};
  state.sketches = sketch_cache{size_t{1} << 20};
  state.synopses_per_type[f.schema][f.id] = f.resident;
  // A non-matching equality predicate prunes the partition only if the Bloom
  // filter is loaded on demand.
  auto absent = state.lookup(unbox(to<expression>("msg == \"absent\"")));
  REQUIRE(absent);
  CHECK_EQUAL(absent->size(), 0u);
  // The sketch was loaded into the cache as a side effect.
  CHECK_NOT_EQUAL(state.sketches.peek(f.id), nullptr);
  // A matching predicate keeps the partition (Bloom filters never yield false
  // negatives).
  auto present = state.lookup(unbox(to<expression>("msg == \"hello\"")));
  REQUIRE(present);
  CHECK_EQUAL(present->size(), 1u);
}

TEST("catalog defers bloom filters of merged partitions when lazy") {
  auto f = fixture{};
  // A freshly flushed/transformed partition arrives with its full Bloom filter.
  auto full = read_mdx(f.mdx, /*lazy=*/false);
  REQUIRE_NOT_EQUAL(full->field_synopses_.at(f.msg), nullptr);
  auto state = catalog_state{};
  state.lazy_sketches = true;
  static_cast<void>(state.merge({{f.id, full}}));
  // The resident synopsis must not retain the Bloom filter, otherwise ongoing
  // ingest would grow resident memory unbounded.
  const auto& stored = state.synopses_per_type.at(f.schema).at(f.id);
  CHECK_EQUAL(stored->field_synopses_.at(f.msg), nullptr);
}

TEST("catalog keeps bloom filters of merged partitions when not lazy") {
  auto f = fixture{};
  auto full = read_mdx(f.mdx, /*lazy=*/false);
  auto state = catalog_state{}; // lazy_sketches defaults to false
  static_cast<void>(state.merge({{f.id, full}}));
  const auto& stored = state.synopses_per_type.at(f.schema).at(f.id);
  CHECK_NOT_EQUAL(stored->field_synopses_.at(f.msg), nullptr);
}

TEST("catalog does not load sketches for non-bloom-answerable predicates") {
  auto f = fixture{};
  auto state = catalog_state{};
  state.sketches = sketch_cache{size_t{1} << 20};
  state.synopses_per_type[f.schema][f.id] = f.resident;
  // A Bloom filter cannot answer `!=`, so loading its sketch could not prune
  // the query; the partition stays a candidate and nothing is loaded.
  auto result = state.lookup(unbox(to<expression>("msg != \"hello\"")));
  REQUIRE(result);
  CHECK_EQUAL(result->size(), 1u);
  CHECK_EQUAL(state.sketches.peek(f.id), nullptr);
}

TEST("catalog without a sketch budget keeps deferred partitions as "
     "candidates") {
  auto f = fixture{};
  auto state = catalog_state{};
  state.sketches = sketch_cache{0}; // on-demand loading disabled
  state.synopses_per_type[f.schema][f.id] = f.resident;
  // Without loading, the catalog cannot rule the partition out and must keep
  // it as a conservative candidate (correct, just coarser).
  auto absent = state.lookup(unbox(to<expression>("msg == \"absent\"")));
  REQUIRE(absent);
  CHECK_EQUAL(absent->size(), 1u);
  CHECK_EQUAL(state.sketches.peek(f.id), nullptr);
}
