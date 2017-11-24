#include "arrow/table.h"
#include "plasma/common.h"

#include "vast/error.hpp"
#include "vast/logger.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"

#include "vast/format/arrow.hpp"

namespace vast {

namespace {

// TODO: write a function to transpose a vector of events such that it can be
// copied into a plasma object.
//std::shared_ptr<arrow::Table> transpose(const std::vector<event>& xs) {
//  std::vector<std::shared_ptr<arrow::ArrayData>> columns;
//  arrow::RecordBatch batch{nullptr, 0, std::move(columns)};
//}

} // namespace <anonymous>

namespace format {
namespace arrow {

writer::writer(const std::string& plasma_socket) {
  VAST_DEBUG(name(), "connects to plasma store at", plasma_socket);
  // TODO: change the last retry parameter from 1 to 0 after our PR got merged
  // (https://github.com/apache/arrow/pull/1354).
  auto status = plasma_client_.Connect(plasma_socket, "",
                                       PLASMA_DEFAULT_RELEASE_DELAY, 1);
  connected_ = status.ok();
  if (!connected())
    VAST_ERROR(name(), "failed to connect to plasma store", status.ToString());
}

writer::~writer() {
  auto status = plasma_client_.Disconnect();
  if (!status.ok())
    VAST_ERROR(name(), "failed to disconnect from plasma store");
}

expected<void> writer::write(event const& x) {
  if (!connected())
    return make_error(ec::format_error, "not connected to plasma store");
  // FIXME: For testing purposes, we store the string representation of each
  // event as separate event.
  auto str = to_string(x);
  auto oid = plasma::ObjectID::from_random();
  uint8_t* buffer;
  auto status = plasma_client_.Create(oid, static_cast<int64_t>(str.size()),
                                      nullptr, 0, &buffer);
  if (!status.ok())
    return make_error(ec::format_error, "failed to create object",
                      status.ToString());
  std::memcpy(buffer, str.data(), str.size());
  status = plasma_client_.Seal(oid);
  if (!status.ok())
    return make_error(ec::format_error, "failed to create object",
                      status.ToString());
  VAST_DEBUG(name(), "sealed object", oid.hex(), "of size", str.size());
  std::cout << oid.hex() << std::endl;
  return no_error;
}

const char* writer::name() const {
  return "arrow-writer";
}

bool writer::connected() const {
  return connected_;
}

} // namespace arrow
} // namespace format
} // namespace vast
