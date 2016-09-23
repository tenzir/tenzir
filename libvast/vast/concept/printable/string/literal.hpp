#ifndef VAST_CONCEPT_PRINTABLE_STRING_LITERAL_HPP
#define VAST_CONCEPT_PRINTABLE_STRING_LITERAL_HPP

#include <string>
#include <type_traits>

#include "vast/concept/printable/string/string.hpp"

namespace vast {

class literal_printer : printer<literal_printer> {
  template <class T>
  using enable_if_non_fp_arithmetic =
    std::enable_if_t<std::is_arithmetic<T>{} && !std::is_floating_point<T>{}>;

  template <class T>
  using enable_if_fp = std::enable_if_t<std::is_floating_point<T>{}>;

public:
  using attribute = unused_type;

  literal_printer(bool b) : str_{b ? "T" : "F"} {
  }

  template <typename T>
  literal_printer(T x, enable_if_non_fp_arithmetic<T>* = nullptr)
    : str_{std::to_string(x)} {
  }

  template <typename T>
  literal_printer(T x, enable_if_fp<T>* = nullptr) : str_{std::to_string(x)} {
    // Remove trailing zeros.
    str_.erase(str_.find_last_not_of('0') + 1, std::string::npos);
  }

  literal_printer(char c) : str_{c} {
  }

  template <size_t N>
  literal_printer(char const(&str)[N]) : str_{str} {
  }

  literal_printer(char const* str) : str_{str} {
  }

  literal_printer(std::string str) : str_(std::move(str)) {
  }

  template <typename Iterator>
  bool print(Iterator& out, unused_type) const {
    return printers::str.print(out, str_);
  }

private:
  std::string str_;
};

namespace printers {

using lit = literal_printer;

} // namespace printers
} // namespace vast

#endif
