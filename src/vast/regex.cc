#include "vast/regex.h"

#include "vast/io/serialization.h"

namespace vast {

regex regex::glob(std::string const& str)
{
#ifdef VAST_CLANG
  auto rx = std::regex_replace(str, std::regex("\\*"), ".*");
  rx = std::regex_replace(rx, std::regex("\\?"), ".");
#else
  auto rx = boost::regex_replace(str, boost::regex("\\*"), ".*");
  rx = boost::regex_replace(rx, boost::regex("\\?"), ".");
#endif
  return regex(rx);
}

regex::regex(string str)
  : rx_(str.begin(), str.end()),
    str_(std::move(str))
{
}

bool regex::match(std::string const& str,
                  std::function<void(std::string const&)> f) const
{
#ifdef VAST_CLANG
  std::sregex_token_iterator i(str.begin(), str.end(), rx_);
  std::sregex_token_iterator const end;
#else
  boost::sregex_token_iterator i(str.begin(), str.end(), rx_);
  boost::sregex_token_iterator const end;
#endif

  if (i == end)
    return false;
  while (i != end)
  {
    f(*i);
    ++i;
  }
  return true;
}

void regex::serialize(io::serializer& sink)
{
  VAST_ENTER(VAST_THIS);
  sink << str_;
}

void regex::deserialize(io::deserializer& source)
{
  VAST_ENTER();
  source >> str_;
  rx_.assign(str_.begin(), str_.end());
  VAST_LEAVE(VAST_THIS);
}

bool operator==(regex const& x, regex const& y)
{
  return x.str_ == y.str_;
}

bool operator<(regex const& x, regex const& y)
{
  return x.str_ < y.str_;
}

std::string to_string(regex const& rx)
{
  std::string str;
  str += '/';
  str += to_string(rx.str_);
  str += '/';
  return str;
}

std::ostream& operator<<(std::ostream& out, regex const& r)
{
  out << '/' << r.str_ << '/';
  return out;
}

} // namespace vast
