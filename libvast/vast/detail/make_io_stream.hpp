#ifndef VAST_DETAIL_MAKE_IO_STREAM_HPP
#define VAST_DETAIL_MAKE_IO_STREAM_HPP

#include <iostream>
#include <string>

#include "vast/expected.hpp"

namespace vast {
namespace detail {

expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, bool is_uds = false);

expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input, bool is_uds = false);

} // namespace detail
} // namespace vast

#endif

