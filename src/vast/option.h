#ifndef VAST_OPTION_HPP
#define VAST_OPTION_HPP

#include <cppa/option.hpp>

namespace vast {

/// An optional value of `T`.
template <typename T>
using option = cppa::option<T>;

} // namespace vast

#endif
