#ifndef VAST_UTIL_HTTP
#define VAST_UTIL_HTTP

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

/// A http_url data type.
class http_url
{
public:


  /// Default-constructs a null JSON value.
  http_url() = default;

  std::vector<std::string> Path()
  {
    return path_;
  }

  void add_path_segment(std::string path_segment)
  {
	  path_.push_back(path_segment);
  }

  std::map<std::string, std::string> Options()
  {
    return options_;
  }

  std::string Options(std::string key)
  {
    return options_[key];
  }

  void add_option(std::string key, std::string value)
  {
	  options_[key] = value;
  }

  bool contains_option(std::string key)
  {
    return options_.count(key) > 0;
    //if (options_.find(key) != options_.end())
    //{
    //  return true;
    //}
    //return false;
  }

private:
  std::vector<std::string> path_;
  std::map<std::string, std::string> options_;

};


} // namespace util
} // namespace vast

#endif
