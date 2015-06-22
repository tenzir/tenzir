#ifndef VAST_UTIL_JSON
#define VAST_UTIL_JSON

#include <map>
#include <string>
#include <vector>
#include "vast/util/none.h"
#include "vast/util/print.h"
#include "vast/util/operators.h"
#include "vast/util/string.h"
#include "vast/util/variant.h"

namespace vast {
namespace util {

/// A http_request data type.
class http_request
{
public:


  /// Default-constructs a null JSON value.
  http_request() = default;

  http_request(std::string method, std::string url, std::string http_version)
    : method_(method),
	  url_(url),
	  http_version_(http_version)
  {
  }

  std::string Method()
  {
    return method_;
  }

  std::string URL()
  {
    return url_;
  }

  std::string HTTP_version()
  {
    return http_version_;
  }

  std::map<std::string, std::string> Header()
  {
    return header_;
  }

  std::string Header(std::string key)
  {
    return header_[key];
  }

  void add_header_field(std::string key, std::string value)
  {
    header_[key] = value;
  }

private:
  std::string method_;
  std::string url_;
  std::string http_version_;
  std::map<std::string, std::string> header_;

};

} // namespace util
} // namespace vast

#endif
