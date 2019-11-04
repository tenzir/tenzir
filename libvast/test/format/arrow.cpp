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

#define SUITE arrow

#include "vast/format/arrow.hpp"

#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/event.hpp"
#include "vast/table_slice_header.hpp"
#include "vast/to_events.hpp"

#include <caf/sum_type.hpp>

#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/memory_pool.h>

#include <utility>

using caf::get;

using namespace std::chrono;
using namespace std::string_literals;
using namespace vast;

#define REQUIRE_OK(expr) REQUIRE(expr.ok());

// Needed to initialize the table slice builder factories.
FIXTURE_SCOPE(arrow_tests, fixtures::events)

TEST(arrow batch) {
  // Create a writer with a buffered output stream.
  format::arrow::writer writer;
  std::shared_ptr<arrow::io::BufferOutputStream> stream;
  REQUIRE_OK(arrow::io::BufferOutputStream::Create(
    1024, arrow::default_memory_pool(), &stream));
  writer.out(stream);
  // Write conn log slices (as record batches) to the stream.
  for (auto& slice : zeek_conn_log_slices)
    writer.write(*slice);
  // Cause the writer to close its current Arrow writer.
  writer.layout(record_type{});
  // Deserialize record batches, store them in arrow_table_slice objects, and
  // compare to the original slices.
  std::shared_ptr<arrow::Buffer> buf;
  REQUIRE_OK(stream->Finish(&buf));
  arrow::io::BufferReader input_straem{buf};
  std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
  REQUIRE_OK(arrow::ipc::RecordBatchStreamReader::Open(&input_straem, &reader));
  auto layout = zeek_conn_log_slices[0]->layout();
  auto arrow_schema = arrow_table_slice_builder::make_arrow_schema(layout);
  size_t slice_id = 0;
  std::shared_ptr<arrow::RecordBatch> batch;
  while (reader->ReadNext(&batch).ok() && batch != nullptr) {
    REQUIRE_LESS(slice_id, zeek_conn_log_slices.size());
    table_slice_header hdr{layout, zeek_conn_log_slices[slice_id]->rows(),
                           zeek_conn_log_slices[slice_id]->offset()};
    CHECK_EQUAL(detail::narrow<size_t>(batch->num_rows()), hdr.rows);
    CHECK(batch->schema()->Equals(*arrow_schema));
    auto slice = caf::make_counted<arrow_table_slice>(std::move(hdr), batch);
    CHECK_EQUAL(*slice, *zeek_conn_log_slices[slice_id]);
    ++slice_id;
  }
  CHECK_EQUAL(slice_id, zeek_conn_log_slices.size());
}

FIXTURE_SCOPE_END()
