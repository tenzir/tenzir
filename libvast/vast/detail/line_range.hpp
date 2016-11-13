#ifndef VAST_DETAIL_LINE_RANGE_HPP
#define VAST_DETAIL_LINE_RANGE_HPP

#include <cstdint>
#include <istream>
#include <string>

#include "vast/detail/range.hpp"

namespace vast {
namespace detail {

// A range of non-empty lines, extracted via `std::getline`.
class line_range : range_facade<line_range> {
public:
  line_range(std::istream& input);

  std::string const& get() const;

  void next();

  bool done() const;

  std::string& line();

  size_t line_number() const;

private:
  std::istream& input_;
  std::string line_;
  size_t line_number_ = 0;
};

} // namespace detail
} // namespace vast

#endif
