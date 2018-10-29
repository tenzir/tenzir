#include "vast/event.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/format/arrow.hpp"

#include "arrow/api.h"
#include "arrow/builder.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "plasma/client.h"
#include "plasma/common.h"

#define SUITE format
#include "fixtures/events.hpp"
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(arrow_tests, fixtures::events)

namespace {

Status get_arrow_batch(plasma::ObjectID id,
                       std::shared_ptr<::arrow::RecordBatch>& b) {
  plasma::PlasmaClient plasma_client_;
  // Connect plasma client
  ARROW_RETURN_NOT_OK(
    plasma_client_.Connect("/tmp/plasma", ""));
  // load Batch from plasma
  plasma::ObjectBuffer buf;
  auto status = plasma_client_.Get(&id, 1, 10, &buf);
  if (status.IsPlasmaObjectNonexistent())
    return Status::PlasmaObjectNonexistent("Object does not exist");
  auto ab = std::shared_ptr<Buffer>{buf.data};
  arrow::io::BufferReader bufferReader{ab};
  std::shared_ptr<arrow::ipc::RecordBatchReader> br;
  ARROW_RETURN_NOT_OK(
    arrow::ipc::RecordBatchStreamReader::Open(&bufferReader, &br));
  status = br->ReadNext(&b);
  return Status::OK();
}

} // namespace

TEST(Arrow Bro log writer) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  for (auto logs : {&bro_conn_log, &bro_http_log, &bro_dns_log}) {
    std::vector<plasma::ObjectID> oids;
    CHECK(writer.write(*logs, oids));
    CHECK(writer.flush());
    REQUIRE_GREATER(oids.size(), 0);
    plasma::PlasmaClient client;
    ARROW_CHECK_OK(
      client.Connect("/tmp/plasma", ""));
    ARROW_CHECK_OK(client.Disconnect());
    std::shared_ptr<arrow::RecordBatch> b;
    auto status = get_arrow_batch(oids.at(0), b);
    CHECK(status.ok());
    CHECK(b);
    CHECK_EQUAL(oids.size(), logs->size());
    auto& xs = caf::get<std::vector<data>>((*logs)[0].data());
    auto col1 = std::make_shared<::arrow::StringArray>(b->column(1)->data());
    CHECK_EQUAL(col1->GetString(0), caf::get<std::string>(xs[1]));
  }
}

TEST(Arrow writer random) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  for (auto& x : random)
    CHECK(writer.write(x));
  CHECK(writer.flush());
}

FIXTURE_SCOPE_END()
