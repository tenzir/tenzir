//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE arrow

#include "vast/format/arrow.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/config.hpp"
#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <caf/sum_type.hpp>

#include <utility>

using namespace std::chrono;
using namespace std::string_literals;
using namespace vast;

#define REQUIRE_OK(expr) REQUIRE((expr).ok());

// Needed to initialize the table slice builder factories.
FIXTURE_SCOPE(arrow_tests, fixtures::events)

TEST(arrow IPC write) {
  // Create a writer with a buffered output stream.
  format::arrow::writer writer;
  std::shared_ptr<arrow::io::BufferOutputStream> stream;
  {
    auto res = arrow::io::BufferOutputStream::Create(
      1024, arrow::default_memory_pool());
    REQUIRE_OK(res);
    stream = *res;
  }
  writer.out(stream);
  // Write conn log slices (as record batches) to the stream.
  for (auto& slice : zeek_conn_log)
    writer.write(slice);

  // closing the stream so we can start reading back the data.
  REQUIRE_OK(stream->Close());

  // Deserialize record batches, store them in arrow_table_slice objects, and
  // compare to the original slices.
  std::shared_ptr<arrow::Buffer> buf;
  {
    auto res = stream->Finish();
    REQUIRE_OK(res);
    buf = *res;
  }
  arrow::io::BufferReader input_stream{buf};
  auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(&input_stream);
  REQUIRE_OK(reader_result);
  auto reader = *reader_result;
  auto&& layout = zeek_conn_log[0].layout();
  auto arrow_schema = layout.to_arrow_schema();
  size_t slice_id = 0;
  std::shared_ptr<arrow::RecordBatch> batch;
  while (reader->ReadNext(&batch).ok() && batch != nullptr) {
    REQUIRE_LESS(slice_id, zeek_conn_log.size());
    auto slice = rebuild(zeek_conn_log[slice_id], table_slice_encoding::arrow);
    CHECK_EQUAL(detail::narrow<size_t>(batch->num_rows()), slice.rows());
    CHECK(batch->schema()->Equals(*arrow_schema));
    CHECK_EQUAL(slice, zeek_conn_log[slice_id]);
    ++slice_id;
  }
  CHECK_EQUAL(slice_id, zeek_conn_log.size());
}

TEST(arrow IPC read) {
  auto stream
    = arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool())
        .ValueOrDie();
  format::arrow::writer writer;
  writer.out(stream);
  for (auto& slice : zeek_conn_log)
    writer.write(slice);
  auto data = stream->Finish().ValueOrDie()->ToString();
  auto in = std::make_unique<std::istringstream>(std::string{data});
  auto options = caf::settings{};
  format::arrow::reader reader{options, std::move(in)};
  auto slices = std::vector<table_slice>{};
  auto add_slice = [&](table_slice slice) {
    slices.emplace_back(std::move(slice));
  };
  reader.read(1 << 16, 1 << 16, add_slice);
  CHECK_EQUAL(zeek_conn_log.size(), slices.size());
  CHECK_EQUAL(zeek_conn_log, slices);
}

FIXTURE_SCOPE_END()
