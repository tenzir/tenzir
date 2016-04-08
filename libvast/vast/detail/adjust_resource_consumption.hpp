#ifndef VAST_DETAIL_ADJUST_RESOURCE_CONSUMPTION_HPP
#define VAST_DETAIL_ADJUST_RESOURCE_CONSUMPTION_HPP

namespace vast {
namespace detail {

/// Adjust the the process' resource consumption in a manner suitable for VAST.
/// @returns `true` on success.
bool adjust_resource_consumption();

} // namespace detail
} // namespace vast

#endif
