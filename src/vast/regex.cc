#include "vast/regex.h"

#include "vast/serialization.h"

namespace vast {

regex regex::glob(std::string const& str)
{
  auto rx = std::regex_replace(str, std::regex("\\*"), ".*");
  return {std::regex_replace(rx, std::regex("\\?"), ".")};
}

regex::regex(string str)
  : rx_(str.begin(), str.end()),
    str_(std::move(str))
{
}

bool regex::match(std::string const& str,
           std::function<void(std::string const&)> f) const
{
  std::sregex_token_iterator i{str.begin(), str.end(), rx_};
  std::sregex_token_iterator const end;

  while (i != end)
  {
    f(i->str());
    ++i;
  }

  return true;
}

void regex::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << str_;
}

void regex::deserialize(deserializer& source)
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

} // namespace vast
