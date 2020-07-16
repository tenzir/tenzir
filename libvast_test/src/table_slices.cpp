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

#include "fixtures/table_slices.hpp"

#include "vast/chunk.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/span.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/value_index.hpp"

using namespace vast;

namespace fixtures {

namespace {

} // namespace <anonymous>

table_slices::table_slices() {
  // Register factories.
  factory<table_slice>::initialize();
  // Define our test layout.
  layout = record_type{
    {"a", bool_type{}},
    {"b", integer_type{}},
    {"c", count_type{}},
    {"d", real_type{}},
    {"e", duration_type{}},
    {"f", time_type{}},
    {"g", string_type{}},
    {"h", pattern_type{}},
    {"i", address_type{}},
    {"j", subnet_type{}},
    {"k", port_type{}},
    {"l", vector_type{count_type{}}},
    {"m", set_type{bool_type{}}},
    {"n", map_type{count_type{}, bool_type{}}},
    // test_vectors
    {"va", vector_type{bool_type{}}},
    {"vb", vector_type{integer_type{}}},
    {"vc", vector_type{count_type{}}},
    {"vd", vector_type{real_type{}}},
    {"ve", vector_type{duration_type{}}},
    {"vf", vector_type{time_type{}}},
    {"vg", vector_type{string_type{}}},
    {"vh", vector_type{pattern_type{}}},
    {"vi", vector_type{address_type{}}},
    {"vj", vector_type{subnet_type{}}},
    {"vk", vector_type{port_type{}}},
    // {"vl", vector_type{vector_type{count_type{}}}},
    // {"vm", vector_type{set_type{bool_type{}}}},
    // {"vn", vector_type{map_type{count_type{}, bool_type{}}}},
    // -- test_sets
    {"sa", set_type{bool_type{}}},
    {"sb", set_type{integer_type{}}},
    {"sc", set_type{count_type{}}},
    {"sd", set_type{real_type{}}},
    {"se", set_type{duration_type{}}},
    {"sf", set_type{time_type{}}},
    {"sg", set_type{string_type{}}},
    {"sh", set_type{pattern_type{}}},
    {"si", set_type{address_type{}}},
    {"sj", set_type{subnet_type{}}},
    {"sk", set_type{port_type{}}},
    // {"sl", set_type{vector_type{count_type{}}}},
    // {"sm", set_type{set_type{bool_type{}}}},
    // {"sn", set_type{map_type{count_type{}, bool_type{}}}},
    // -- test_maps_left
    {"maa", map_type{bool_type{}, bool_type{}}},
    {"mba", map_type{integer_type{}, bool_type{}}},
    {"mca", map_type{count_type{}, bool_type{}}},
    {"mda", map_type{real_type{}, bool_type{}}},
    {"mea", map_type{duration_type{}, bool_type{}}},
    {"mfa", map_type{time_type{}, bool_type{}}},
    {"mga", map_type{string_type{}, bool_type{}}},
    {"mha", map_type{pattern_type{}, bool_type{}}},
    {"mia", map_type{address_type{}, bool_type{}}},
    {"mja", map_type{subnet_type{}, bool_type{}}},
    {"mka", map_type{port_type{}, bool_type{}}},
    // {"mla", map_type{vector_type{count_type{}}, bool_type{}}},
    // {"mma", map_type{set_type{bool_type{}}, bool_type{}}},
    // {"mna", map_type{map_type{count_type{}, bool_type{}}, bool_type{}}},
    // -- test_maps_right (intentionally no maa)
    {"mab", map_type{bool_type{}, integer_type{}}},
    {"mac", map_type{bool_type{}, count_type{}}},
    {"mad", map_type{bool_type{}, real_type{}}},
    {"mae", map_type{bool_type{}, duration_type{}}},
    {"maf", map_type{bool_type{}, time_type{}}},
    {"mag", map_type{bool_type{}, string_type{}}},
    {"mah", map_type{bool_type{}, pattern_type{}}},
    {"mai", map_type{bool_type{}, address_type{}}},
    {"maj", map_type{bool_type{}, subnet_type{}}},
    {"mak", map_type{bool_type{}, port_type{}}},
    // {"mal", map_type{bool_type{}, vector_type{count_type{}}}},
    // {"mam", map_type{bool_type{}, set_type{bool_type{}}}},
    // {"man", map_type{bool_type{}, map_type{count_type{}, bool_type{}}}},
  }.name("test");
  // A bunch of test data for nested type combinations.
  // clang-format off
  auto test_vectors = ""s
    + ", [T]"s // va
    + ", [+7]"s // vb
    + ", [42]"s // vc
    + ", [4.2]"s // vd
    + ", [1337ms]"s // ve
    + ", [2018-12-24]"s // vf
    + ", [\"foo\"]"s // vg
    + ", [/foo.*bar/]"s // vh
    + ", [127.0.0.1]"s // vi
    + ", [10.0.0.0/8]"s // vj
    + ", [80/tcp]"s // vk
    // + ", [[1, 2, 3]]"s // vl
    // + ", [{T, F}]"s // vm
    // + ", [{1 -> T, 2 -> F, 3 -> T}]"s // vn
    ;
  auto test_sets = ""s
    + ", {T}"s // sa
    + ", {+7}"s // sb
    + ", {42}"s // sc
    + ", {4.2}"s // sd
    + ", {1337ms}"s // se
    + ", {2018-12-24}"s // sf
    + ", {\"foo\"}"s // sg
    + ", {/foo.*bar/}"s // sh
    + ", {127.0.0.1}"s // si
    + ", {10.0.0.0/8}"s // sj
    + ", {80/tcp}"s // sk
    // + ", {[1, 2, 3]}"s // sl
    // + ", {{T, F}}"s // sm
    // + ", {{1 -> T, 2 -> F, 3 -> T}}"s // sn
    ;
  auto test_maps_left = ""s
    + ", {T -> T}"s // maa
    + ", {+7 -> T}"s // mba
    + ", {42 -> T}"s // mca
    + ", {4.2 -> T}"s // mda
    + ", {1337ms -> T}"s // mea
    + ", {2018-12-24 -> T}"s // mfa
    + ", {\"foo\" -> T}"s // mga
    + ", {/foo.*bar/ -> T}"s // mha
    + ", {127.0.0.1 -> T}"s // mia
    + ", {10.0.0.0/8 -> T}"s // mja
    + ", {80/tcp -> T}"s // mka
    // + ", {[1, 2, 3] -> T}"s // mla
    // + ", {{T, F} -> T}"s // mma
    // + ", {{1 -> T, 2 -> F, 3 -> T} -> T}"s // mna
    ;
  auto test_maps_right = ""s
    // (intentionally no maa)
    + ", {T -> +7}"s // mab
    + ", {T -> 42}"s // mac
    + ", {T -> 4.2}"s // mad
    + ", {T -> 1337ms}"s // mae
    + ", {T -> 2018-12-24}"s // maf
    + ", {T -> \"foo\"}"s // mag
    + ", {T -> /foo.*bar/}"s // mah
    + ", {T -> 127.0.0.1}"s // mai
    + ", {T -> 10.0.0.0/8}"s // maj
    + ", {T -> 80/tcp}"s // mak
    // + ", {T -> [1, 2, 3]}"s // mal
    // + ", {T -> {T, F}}"s // mam
    // + ", {T -> {1 -> T, 2 -> F, 3 -> T}}"s // man
    ;
  auto test_collections
    = test_vectors
    + test_sets
    + test_maps_left
    + test_maps_right
    ;
  // clang-format on
  // Initialize test data.
  auto rows = std::vector<std::string>{
    "[T, +7, 42, 4.2, 1337ms, 2018-12-24, \"foo\", /foo.*bar/, 127.0.0.1,"
    " 10.0.0.0/8, 80/tcp, [1, 2, 3], {T, F}, {1 -> T, 2 -> F, 3 -> T}"
      + test_collections + "]",
    "[F, -7, 43, 0.42, -1337ms, 2018-12-25, \"bar\", nil, ::1, 64:ff9b::/96,"
    " 53/udp, [], {}, {-}"
      + test_collections + "]",
  };
  for (auto& row : rows) {
    auto xs = unbox(to<data>(row));
    test_data.push_back(caf::get<vector>(xs));
  }
}

void table_slices::run() {
  if (builder == nullptr)
    FAIL("no valid builder found; missing fixture initialization?");
  test_add();
  test_equality();
  test_copy();
  test_manual_serialization();
  test_smart_pointer_serialization();
  test_message_serialization();
  test_load_from_chunk();
  test_append_column_to_index();
}

caf::binary_deserializer table_slices::make_source() {
  return caf::binary_deserializer{sys, buf};
}

caf::binary_serializer table_slices::make_sink() {
  buf.clear();
  return caf::binary_serializer{sys, buf};
}

table_slice_ptr table_slices::make_slice() {
  for (auto& xs : test_data)
    for (auto& x : xs)
      if (!builder->add(make_view(x)))
        FAIL("builder failed to add element");
  return builder->finish();
}

vast::data_view table_slices::at(size_t row, size_t col) const {
  VAST_ASSERT(row < test_data.size());
  VAST_ASSERT(col < test_data[row].size());
  return make_view(test_data[row][col]);
}

void table_slices::test_add() {
  MESSAGE(">> test table_slice_builder::add");
  auto slice = make_slice();
  CHECK_EQUAL(slice->rows(), 2u);
  CHECK_EQUAL(slice->columns(), layout.fields.size());
  for (size_t row = 0; row < slice->rows(); ++row)
    for (size_t col = 0; col < slice->columns(); ++col) {
      MESSAGE("checking value at (" << row << ',' << col << ')');
      CHECK_EQUAL(slice->at(row, col), at(row, col));
    }
}

void table_slices::test_equality() {
  MESSAGE(">> test equality");
  auto slice1 = make_slice();
  auto slice2 = make_slice();
  CHECK_EQUAL(*slice1, *slice2);
}

void table_slices::test_copy() {
  MESSAGE(">> test copy");
  auto slice1 = make_slice();
  table_slice_ptr slice2{slice1->copy(), false};
  CHECK_EQUAL(*slice1, *slice2);
}

void table_slices::test_manual_serialization() {
  MESSAGE(">> test manual serialization via inspect");
  MESSAGE("make slices");
  auto slice1 = make_slice();
  table_slice_ptr slice2;
  MESSAGE("save content of the first slice into the buffer");
  auto sink = make_sink();
  CHECK_EQUAL(inspect(sink, slice1), caf::none);
  MESSAGE("load content for the second slice from the buffer");
  auto source = make_source();
  CHECK_EQUAL(inspect(source, slice2), caf::none);
  MESSAGE("check result of serialization roundtrip");
  REQUIRE_NOT_EQUAL(slice2, nullptr);
  CHECK_EQUAL(*slice1, *slice2);
}

void table_slices::test_smart_pointer_serialization() {
  MESSAGE(">> test smart pointer serialization");
  MESSAGE("make slices");
  auto slice1 = make_slice();
  table_slice_ptr slice2;
  MESSAGE("save content of the first slice into the buffer");
  auto sink = make_sink();
  CHECK_EQUAL(sink(slice1), caf::none);
  MESSAGE("load content for the second slice from the buffer");
  auto source = make_source();
  CHECK_EQUAL(source(slice2), caf::none);
  MESSAGE("check result of serialization roundtrip");
  REQUIRE_NOT_EQUAL(slice2, nullptr);
  CHECK_EQUAL(*slice1, *slice2);
}

void table_slices::test_message_serialization() {
  MESSAGE(">> test message serialization");
  MESSAGE("make slices");
  auto slice1 = caf::make_message(make_slice());
  caf::message slice2;
  MESSAGE("save content of the first slice into the buffer");
  auto sink = make_sink();
  CHECK_EQUAL(sink(slice1), caf::none);
  MESSAGE("load content for the second slice from the buffer");
  auto source = make_source();
  CHECK_EQUAL(source(slice2), caf::none);
  MESSAGE("check result of serialization roundtrip");
  REQUIRE(slice2.match_elements<table_slice_ptr>());
  CHECK_EQUAL(*slice1.get_as<table_slice_ptr>(0),
              *slice2.get_as<table_slice_ptr>(0));
  CHECK_EQUAL(slice2.get_as<table_slice_ptr>(0)->implementation_id(),
              builder->implementation_id());
}

void table_slices::test_load_from_chunk() {
  MESSAGE(">> test load from chunk");
  auto slice1 = make_slice();
  auto sink = make_sink();
  CHECK_EQUAL(sink(slice1), caf::none);
  auto chk = chunk::make(as_bytes(span{buf.data(), buf.size()}));
  auto slice2 = factory<table_slice>::traits::make(chk);
  REQUIRE_NOT_EQUAL(slice2, nullptr);
  CHECK_EQUAL(*slice1, *slice2);
}

void table_slices::test_append_column_to_index() {
  MESSAGE(">> test append_column_to_index");
  auto idx = factory<value_index>::make(integer_type{}, caf::settings{});
  REQUIRE_NOT_EQUAL(idx, nullptr);
  auto slice = make_slice();
  slice->append_column_to_index(1, *idx);
  CHECK_EQUAL(idx->offset(), 2u);
  constexpr auto less = relational_operator::less;
  CHECK_EQUAL(unbox(idx->lookup(less, make_view(3))), make_ids({1}));
}

} // namespace fixtures
