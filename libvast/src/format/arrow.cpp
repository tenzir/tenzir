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
  auto status = plasma_client_.Connect(plasma_socket, "",
                                       PLASMA_DEFAULT_RELEASE_DELAY, 0);
  connected_ = status.ok();
  if (!connected())
    VAST_ERROR(name(), "failed to connect to plasma store", status.ToString());
}

writer::~writer() {
  auto status = plasma_client_.Disconnect();
  if (!status.ok())
    VAST_ERROR(name(), "failed to disconnect from plasma store");
}

expected<void> writer::write(const std::vector<event>& /* xs */) {
  // TODO: Implement this function.
  return no_error;
}

expected<void> writer::write(const event& x) {
  if (!connected())
    return make_error(ec::format_error, "not connected to plasma store");
  // FIXME: For testing purposes, we store the string representation of each
  // event as separate event.
  auto str = to_string(x);
  auto oid = make_object(str.data(), str.size());
  if (!oid)
    return oid;
  std::cout << oid->hex() << std::endl;
  return no_error;
}

const char* writer::name() const {
  return "arrow-writer";
}

bool writer::connected() const {
  return connected_;
}

expected<plasma::ObjectID>
writer::make_object(const void* data, size_t size) {
  auto oid = plasma::ObjectID::from_random();
  uint8_t* buffer;
  auto status = plasma_client_.Create(oid, static_cast<int64_t>(size),
                                      nullptr, 0, &buffer);
  if (!status.ok())
    return make_error(ec::format_error, "failed to create object",
                      status.ToString());
  std::memcpy(buffer, reinterpret_cast<const char*>(data), size);
  status = plasma_client_.Seal(oid);
  if (!status.ok())
    return make_error(ec::format_error, "failed to create object",
                      status.ToString());
  VAST_DEBUG(name(), "sealed object", oid.hex(), "of size", size);
  return oid;
}

} // namespace arrow
} // namespace format
} // namespace vast
