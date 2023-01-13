//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/atoms.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/binary_serializer.hpp>

#include <string>
#include <tuple>
#include <vector>

/// Helper macro to define a table-slice unit test.
#define TEST_TABLE_SLICE(builder, id)                                          \
  TEST(type) {                                                                 \
    initialize();                                                              \
    run();                                                                     \
  }

namespace vast {

/// Constructs table slices filled with random content for testing purposes.
/// @param num_slices The number of table slices to generate.
/// @param slice_size The number of rows per table slices.
/// @param schema The schema of the table slice.
/// @param offset The offset of the first table slize.
/// @param seed The seed value for initializing the random-number generator.
/// @returns a list of randomnly filled table slices or an error.
/// @relates table_slice
caf::expected<std::vector<table_slice>>
make_random_table_slices(size_t num_slices, size_t slice_size, type schema,
                         id offset = 0, size_t seed = 0);

/// Converts the table slice into a 2-D matrix in row-major order such that
/// each row represents an event.
/// @param slice The table slice to convert.
/// @param first_row An offset to the first row to consider.
/// @param num_rows Then number of rows to consider. (0 = all rows)
/// @returns a 2-D matrix of data instances corresponding to *slice*.
/// @requires first_row < slice.rows()
/// @requires num_rows <= slice.rows() - first_row
/// @note This function exists primarily for unit testing because it performs
/// excessive memory allocations.
std::vector<std::vector<data>>
make_data(const table_slice& slice, size_t first_row = 0, size_t num_rows = 0);

std::vector<std::vector<data>>
make_data(const std::vector<table_slice>& slices);

} // namespace vast

namespace fixtures {

class table_slices : public deterministic_actor_system_and_events {
public:
  explicit table_slices(std::string_view suite);

  /// Registers a table slice implementation.
  void initialize() {
    using namespace vast;
    builder = std::make_shared<table_slice_builder>(schema);
    if (builder == nullptr)
      FAIL("builder factory could not construct a valid instance");
  }

  // Run all tests in the fixture.
  void run();

private:
  using triple = std::tuple<vast::integer, std::string, vast::real>;

  caf::binary_serializer make_sink();

  vast::table_slice make_slice();

  vast::data_view at(size_t row, size_t col) const;

  void test_add();

  void test_equality();

  void test_copy();

  void test_manual_serialization();

  void test_smart_pointer_serialization();

  void test_message_serialization();

  void test_append_column_to_index();

  vast::type schema = type{
    "test",
    record_type{
      {"a", bool_type{}},
      {"b", int64_type{}},
      {"c", uint64_type{}},
      {"d", double_type{}},
      {"e", duration_type{}},
      {"f", time_type{}},
      {"g", string_type{}},
      {"h", pattern_type{}},
      {"i", ip_type{}},
      {"j", subnet_type{}},
      {"l", list_type{uint64_type{}}},
      {"n", map_type{uint64_type{}, bool_type{}}},
      // test_lists
      {"va", list_type{bool_type{}}},
      {"vb", list_type{int64_type{}}},
      {"vc", list_type{uint64_type{}}},
      {"vd", list_type{double_type{}}},
      {"ve", list_type{duration_type{}}},
      {"vf", list_type{time_type{}}},
      {"vg", list_type{string_type{}}},
      {"vh", list_type{pattern_type{}}},
      {"vi", list_type{ip_type{}}},
      {"vj", list_type{subnet_type{}}},
      // {"vl", list_type{list_type{uint64_type{}}}},
      // {"vm", list_type{map_type{uint64_type{}, bool_type{}}}},
      // -- test_maps_left
      {"maa", map_type{bool_type{}, bool_type{}}},
      {"mba", map_type{int64_type{}, bool_type{}}},
      {"mca", map_type{uint64_type{}, bool_type{}}},
      {"mda", map_type{double_type{}, bool_type{}}},
      {"mea", map_type{duration_type{}, bool_type{}}},
      {"mfa", map_type{time_type{}, bool_type{}}},
      {"mga", map_type{string_type{}, bool_type{}}},
      {"mha", map_type{pattern_type{}, bool_type{}}},
      {"mia", map_type{ip_type{}, bool_type{}}},
      {"mja", map_type{subnet_type{}, bool_type{}}},
      // {"mla", map_type{list_type{uint64_type{}}, bool_type{}}},
      // {"mna", map_type{map_type{uint64_type{}, bool_type{}}, bool_type{}}},
      // -- test_maps_right (intentionally no maa)
      {"mab", map_type{bool_type{}, int64_type{}}},
      {"mac", map_type{bool_type{}, uint64_type{}}},
      {"mad", map_type{bool_type{}, double_type{}}},
      {"mae", map_type{bool_type{}, duration_type{}}},
      {"maf", map_type{bool_type{}, time_type{}}},
      {"mag", map_type{bool_type{}, string_type{}}},
      {"mah", map_type{bool_type{}, pattern_type{}}},
      {"mai", map_type{bool_type{}, ip_type{}}},
      {"maj", map_type{bool_type{}, subnet_type{}}},
      // {"mal", map_type{bool_type{}, list_type{uint64_type{}}}},
      // {"man", map_type{bool_type{}, map_type{uint64_type{}, bool_type{}}}},
      {"aas", type{"aas", type{"as", string_type{}}}},
    },
  };

  vast::table_slice_builder_ptr builder;

  std::vector<std::vector<vast::data>> test_data;

  caf::byte_buffer buf;
};

} // namespace fixtures
