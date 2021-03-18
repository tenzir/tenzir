// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE msgpack_table_slice

#include "vast/msgpack_table_slice.hpp"

#include "vast/test/fixtures/table_slices.hpp"
#include "vast/test/test.hpp"

#include "vast/msgpack_table_slice_builder.hpp"

using namespace vast;

FIXTURE_SCOPE(msgpack_table_slice_tests, fixtures::table_slices)

TEST_TABLE_SLICE(msgpack_table_slice_builder, msgpack)

FIXTURE_SCOPE_END()
