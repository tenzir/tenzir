#include "vast/error.hpp"
#include "vast/logger.hpp"

#include "vast/format/arrow.hpp"

namespace vast {
namespace format {
namespace arrow {

writer::writer(const std::string& plasma_socket) {
  VAST_DEBUG(name(), "connects to plasma store at", plasma_socket);
  auto status = plasma_client_.Connect(plasma_socket, "",
                                       PLASMA_DEFAULT_RELEASE_DELAY, 0);
  connected_ = status.ok();
  if (!connected_)
    VAST_ERROR(name(), "failed to connect to plasma store");
}

writer::~writer() {
  auto status = plasma_client_.Disconnect();
  if (!status.ok())
    VAST_ERROR(name(), "failed to disconnect from plasma store");
}

expected<void> writer::write(event const& /* e */) {
  if (!connected())
    return make_error(ec::format_error, "not connected to plasma store");
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
