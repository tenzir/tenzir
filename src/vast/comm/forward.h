#ifndef VAST_COMM_FORWARD_H
#define VAST_COMM_FORWARD_H

#include <functional>
#include <memory>
#include <ze/forward.h>

namespace vast {
namespace comm {

class connection;
class io;

typedef std::shared_ptr<connection> connection_ptr;
typedef std::function<void(connection_ptr const&)> conn_handler;
typedef std::function<void(std::shared_ptr<ze::event> const& event)> event_handler;


} // namespace comm
} // namespace vast

#endif
