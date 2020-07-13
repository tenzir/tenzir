// Copyright Tenzir GmbH. All rights reserved.

#define SUITE msgpack_table_slice

#include "vast/msgpack_table_slice.hpp"

#include "vast/msgpack_table_slice_builder.hpp"

#include <vast/test/fixtures/table_slices.hpp>
#include <vast/test/test.hpp>

using namespace vast;

FIXTURE_SCOPE(msgpack_table_slice_tests, fixtures::table_slices)

TEST_TABLE_SLICE(msgpack_table_slice)

FIXTURE_SCOPE_END()
