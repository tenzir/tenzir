#ifndef VAST_DETAIL_DEMANGLE_H
#define VAST_DETAIL_DEMANGLE_H

#include <string>
#include <typeinfo>

namespace vast {
namespace detail {

/// Demangles the implementation-specific type name from `std::type_info`.
/// @param typeid_name The value returned by `std::type_info::name`.
/// @param A platform-indpendent represenation of *typeid_name*.
std::string demangle(char const* typeid_name);

/// Demangles the implementation-specific type name from `std::type_info`.
/// @param info An instance of `std::type_info` as obtained by `typeid`.
/// @param A platform-indpendent represenation of *typeid_name*.
std::string demangle(const std::type_info& info);

} // namespace detail
} // namespace vast

#endif
