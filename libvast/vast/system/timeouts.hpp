#ifndef VAST_SYSTEM_TIMEOUTS_HPP
#define VAST_SYSTEM_TIMEOUTS_HPP

#include <chrono>

namespace vast {
namespace system {

/// Timeout when interacting with the consensus module.
constexpr auto consensus_timeout = std::chrono::seconds(10);

} // namespace system
} // namespace vast

#endif
