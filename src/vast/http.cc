#include <cctype>

#include "vast/http.h"

namespace vast {
namespace http {

header const* message::header(std::string const& name) const {
  auto pred = [&](auto& x) -> bool {
    if (x.name.size() != name.size())
      return false;
    for (auto i = 0u; i < name.size(); ++i)
      if (::toupper(x.name[i]) != ::toupper(name[i]))
        return false;
    return true;
  };
  auto i = std::find_if(headers.begin(), headers.end(), pred);
  return i == headers.end() ? nullptr : &*i;
};

} // namspace http
} // namspace vast
