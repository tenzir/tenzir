#include "vast/event.hpp"

#include "vast/format/arrow.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"

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

Status get_arrow_batch(plasma::ObjectID id, std::shared_ptr<::arrow::RecordBatch>& b) {
  plasma::PlasmaClient plasma_client_;
  // Connect plasma client
  ARROW_RETURN_NOT_OK(
  plasma_client_.Connect("/tmp/plasma", "", PLASMA_DEFAULT_RELEASE_DELAY));
  // load Batch from plasma
  plasma::ObjectBuffer pbuf;
  auto status = plasma_client_.Get(&id, 1, 10, &pbuf);
  if (status.IsPlasmaObjectNonexistent()) {
    return Status::PlasmaObjectNonexistent("Object not exist");
  }
  std::shared_ptr<Buffer> ab;
  ab = pbuf.data;
  ::arrow::io::BufferReader bufferReader(ab);
  std::shared_ptr<arrow::ipc::RecordBatchReader> br;
  ARROW_RETURN_NOT_OK(
    arrow::ipc::RecordBatchStreamReader::Open(&bufferReader, &br));
  status = br->ReadNext(&b);
  return Status::OK();
}
TEST(Arrow writer conn) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  std::vector<plasma::ObjectID> oids;
  CHECK(writer.write(bro_conn_log, oids));
  CHECK(writer.flush());
  plasma::PlasmaClient client;
  ARROW_CHECK_OK(
    client.Connect("/tmp/plasma", "", PLASMA_DEFAULT_RELEASE_DELAY));
  ARROW_CHECK_OK(client.Disconnect());
  std::shared_ptr<::arrow::RecordBatch> b;
  auto status = get_arrow_batch(oids.at(0), b);
  CHECK(status.ok());
  CHECK(b);
  CHECK_EQUAL(oids.size(), bro_conn_log.size());
  auto data_v = get<std::vector<data>>(bro_conn_log.at(0).data());
  auto data_a = std::make_shared<::arrow::StringArray>(b->column(1)->data()); 
  CHECK_EQUAL(data_a->GetString(0), get<std::string>(data_v.at(1)));
}
TEST(Arrow writer http) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  std::vector<plasma::ObjectID> oids;
  CHECK(writer.write(bro_http_log, oids));
  CHECK(writer.flush());
  plasma::PlasmaClient client;
  ARROW_CHECK_OK(
    client.Connect("/tmp/plasma", "", PLASMA_DEFAULT_RELEASE_DELAY));
  ARROW_CHECK_OK(client.Disconnect());
  std::shared_ptr<::arrow::RecordBatch> b;
  auto status = get_arrow_batch(oids.at(0), b);
  CHECK(status.ok());
  CHECK(b);
  CHECK_EQUAL(oids.size(), bro_http_log.size());
  auto data_v = get<std::vector<data>>(bro_http_log.at(0).data());
  auto data_a = std::make_shared<::arrow::StringArray>(b->column(1)->data()); 
  CHECK_EQUAL(data_a->GetString(0), get<std::string>(data_v.at(1)));
  /*
  std::cout << to_string(bro_http_log.at(0).data()) << std::endl;
  for (int i = 0; i < b->num_columns(); i++){
    std::cout << b->column(i)->ToString() << std::endl; 
  }
  */
}
TEST(Arrow writer dns) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  std::vector<plasma::ObjectID> oids;
  auto result = writer.write(bro_dns_log, oids);
  CHECK(result);
  result = writer.flush();
  CHECK(result);
  plasma::PlasmaClient client;
  ARROW_CHECK_OK(
    client.Connect("/tmp/plasma", "", PLASMA_DEFAULT_RELEASE_DELAY));
  ARROW_CHECK_OK(client.Disconnect());
  std::shared_ptr<::arrow::RecordBatch> b;
  auto status = get_arrow_batch(oids.at(0), b);
  CHECK(status.ok());
  CHECK(b);
  CHECK_EQUAL(oids.size(), bro_dns_log.size());
  auto data_v = get<std::vector<data>>(bro_dns_log.at(0).data());
  auto data_a = std::make_shared<::arrow::StringArray>(b->column(1)->data()); 
  CHECK_EQUAL(data_a->GetString(0), get<std::string>(data_v.at(1)));
  /*
  std::cout << to_string(bro_dns_log.at(0).data()) << std::endl;
  for (int i = 0; i < b->num_columns(); i++){
    std::cout << b->column(i)->ToString() << std::endl; 
  }
  */
}
TEST(Arrow writer random) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  for (auto& x : random)
    CHECK(writer.write(x));
  CHECK(writer.flush());
}
FIXTURE_SCOPE_END()
