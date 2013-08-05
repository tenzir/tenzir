#include <vast/source/file.h>

#include "vast/io/getline.h"
#include "vast/util/field_splitter.h"
#include "vast/parse.h"
#include "vast/logger.h"

namespace vast {
namespace source {

file::file(std::string const& filename)
  : file_handle_(path(filename)),
    file_stream_(file_handle_)
{
  VAST_ENTER();
  if (! file_handle_.open(vast::file::read_only))
    finished_ = true;
}

bool file::finished()
{
  VAST_ENTER();
  VAST_RETURN(finished_);
}

line::line(std::string const& filename)
  : file(filename)
{
  VAST_ENTER();
  if (! next())
    finished_ = true;
}

option<event> line::extract()
{
  VAST_ENTER();

  option<event> e;
  if (! line_.empty())
    e = parse(line_);

  do
  {
    if (! next())
    {
      finished_ = true;
      return {};
    }
  }
  while (line_.empty());

  return e;
}

bool line::next()
{
  VAST_ENTER();
  bool success;
  if ((success = io::getline(file_stream_, line_)))
    ++current_;
  VAST_RETURN(success);
}

bro2::bro2(std::string const& filename)
  : line(filename)
{
  VAST_ENTER();

  if (! parse_header())
  {
    VAST_LOG_ERROR("not a valid Bro 2.x log file: " << filename);
    finished_ = true;
  }
}

bool bro2::parse_header()
{
  VAST_ENTER();

  while (line_.empty())
    if (! next())
      break;

  util::field_splitter<std::string::const_iterator> fs;
  fs.split(line_.begin(), line_.end());
  if (fs.fields() != 2 || ! fs.equals(0, "#separator"))
  {
    VAST_LOG_ERROR("invalid #separator definition");
    return false;
  }

  {
    std::string sep;
    std::string bro_sep(fs.start(1), fs.end(1));
    std::string::size_type pos = 0;
    while (pos != std::string::npos)
    {
      pos = bro_sep.find("\\x", pos);
      if (pos != std::string::npos)
      {
        auto i = std::stoi(bro_sep.substr(pos + 2, 2), nullptr, 16);
        assert(i >= 0 && i <= 255);
        sep.push_back(i);
        pos += 2;
      }
    }

    separator_ = string(sep.begin(), sep.end());
  }
  if (! next())
    return false;
  {
    util::field_splitter<std::string::const_iterator> fs(separator_.data(),
                                                         separator_.size());
    fs.split(line_.begin(), line_.end());
    if (fs.fields() != 2 || ! fs.equals(0, "#set_separator"))
    {
      VAST_LOG_ERROR("invalid #set_separator definition");
      return false;
    }

    auto set_sep = std::string(fs.start(1), fs.end(1));
    set_separator_ = string(set_sep.begin(), set_sep.end());
  }
  if (! next())
    return false;
  {
    util::field_splitter<std::string::const_iterator> fs(separator_.data(),
                                                         separator_.size());
    fs.split(line_.begin(), line_.end());
    if (fs.fields() != 2 || ! fs.equals(0, "#empty_field"))
    {
      VAST_LOG_ERROR("invalid #empty_field definition");
      return false;
    }

    auto empty = std::string(fs.start(1), fs.end(1));
    empty_field_ = string(empty.begin(), empty.end());
  }
  if (! next())
    return false;
  {
    util::field_splitter<std::string::const_iterator> fs(separator_.data(),
                                                         separator_.size());
    fs.split(line_.begin(), line_.end());
    if (fs.fields() != 2 || ! fs.equals(0, "#unset_field"))
    {
      VAST_LOG_ERROR("invalid #unset_field definition");
      return false;
    }

    auto unset = std::string(fs.start(1), fs.end(1));
    unset_field_ = string(unset.begin(), unset.end());
  }
  if (! next())
    return false;
  {
    util::field_splitter<std::string::const_iterator> fs(separator_.data(),
                                                         separator_.size());
    fs.split(line_.begin(), line_.end());
    if (fs.fields() != 2 || ! fs.equals(0, "#path"))
    {
      VAST_LOG_ERROR("invalid #path definition");
      return false;
    }

    path_ = string(fs.start(1), fs.end(1));
  }
  // Skip #open tag.
  if (! (next() && next()))
    return false;
  {
    util::field_splitter<std::string::const_iterator> fs(separator_.data(),
                                                         separator_.size());
    fs.split(line_.begin(), line_.end());
    if (! fs.equals(0, "#fields"))
    {
      VAST_LOG_ERROR("invalid #fields definition");
      return false;
    }

    for (size_t i = 1; i < fs.fields(); ++i)
      field_names_.emplace_back(fs.start(i), fs.end(i));
  }
  if (! next())
    return false;
  {
    util::field_splitter<std::string::const_iterator> fs(separator_.data(),
                                                         separator_.size());
    fs.split(line_.begin(), line_.end());
    if (! fs.equals(0, "#types"))
    {
      VAST_LOG_ERROR("invalid #types definition");
      return false;
    }

    for (size_t i = 1; i < fs.fields(); ++i)
    {
      string t(fs.start(i), fs.end(i));
      if (t.starts_with("table") || t.starts_with("vector"))
      {
        field_types_.push_back(set_type);
        auto open = t.find("[");
        assert(open != string::npos);
        auto close = t.find("]", open);
        assert(close != string::npos);
        auto elem = t.substr(open + 1, close - open - 1);
        set_types_.push_back(bro_to_vast(elem));
      }
      else
      {
        field_types_.push_back(bro_to_vast(t));
      }
    }
  }

  line_.clear(); // Triggers call to next().

  VAST_LOG_DEBUG(
      "event source @" << id() << " parsed bro2 header:" <<
      " #separator " << separator_ <<
      " #set_separator " << set_separator_ <<
      " #empty_field " << empty_field_ <<
      " #unset_field " << unset_field_ <<
      " #path " << path_);
  {
    std::ostringstream str;
    for (auto& name : field_names_)
      str << " " << name;
    VAST_LOG_DEBUG("event source @" << id() << " has field names:" << str.str());
  }
  {
    std::ostringstream str;
    for (auto& type : field_types_)
      str << " " << type;
    VAST_LOG_DEBUG("event source @" << id() << " has field types:" << str.str());
  }
  if (! set_types_.empty())
  {
    std::ostringstream str;
    for (auto& type : set_types_)
      str << " " << type;
    VAST_LOG_DEBUG("event source @" << id() << " has set types:" << str.str());
  }

  path_ = "bro::" + path_;

  return true;
}

value_type bro2::bro_to_vast(string const& type)
{
  if (type == "enum" || type == "string" || type == "file")
    return string_type;
  else if (type == "bool")
    return bool_type;
  else if (type == "int")
    return int_type;
  else if (type == "count")
    return uint_type;
  else if (type == "double")
    return double_type;
  else if (type == "interval")
    return time_range_type;
  else if (type == "time")
    return time_point_type;
  else if (type == "addr")
    return address_type;
  else if (type == "port")
    return port_type;
  else if (type == "pattern")
    return regex_type;
  else if (type == "subnet")
    return prefix_type;
  else
    return invalid_type;
}

option<event> bro2::parse(std::string const& line)
{
  VAST_ENTER();

  // TODO: switch to grammar-based parsing.
  using vast::parse;
  util::field_splitter<std::string::const_iterator> fs(separator_.data(),
                                                       separator_.size());
  fs.split(line.begin(), line.end());

  if (fs.fields() > 0 && *fs.start(0) == '#')
  {
    VAST_LOG_DEBUG("ignoring commented line: " << line);
    return {};
  }

  if (fs.fields() != field_types_.size())
  {
    VAST_LOG_ERROR("inconsistent number of fields (line " << current_ << ')');
    return {};
  }

  event e;
  e.name(path_);
  e.timestamp(now());
  size_t sets = 0;
  for (size_t f = 0; f < fs.fields(); ++f)
  {
    auto start = fs.start(f);
    auto end = fs.end(f);

    // Check whether the field is set. (Not '-' by default.)
    auto unset = true;
    for (size_t i = 0; i < unset_field_.size(); ++i)
    {
      if (unset_field_[i] != *(start + i))
      {
        unset = false;
        break;
      }
    }
    if (unset)
    {
      e.push_back(nil);
      continue;
    }

    // Check whether the field empty. (Not "(empty)" by default.)
    auto empty = true;
    for (size_t i = 0; i < empty_field_.size(); ++i)
    {
      if (empty_field_[i] != *(start + i))
      {
        empty = false;
        break;
      }
    }
    if (empty)
    {
      e.emplace_back(field_types_[f]);
      continue;
    }

    // Parse the field according to its type, sets are still special.
    if (field_types_[f] == set_type)
    {
      set s;
      // TODO: take care of escaped set separators.
      auto success = parse(start, end, s, set_types_[sets++], set_separator_);
      if (! success)
      {
        VAST_LOG_ERROR("invalid set syntax");
        return {};
      }
      e.emplace_back(std::move(s));
    }
    else
    {
      value v;
      auto success = parse(start, end, v, field_types_[f]);
      if (! success)
      {
        VAST_LOG_ERROR("could not parse field");
        return {};
      }
      e.push_back(std::move(v));
    }
  }

  return e;
}

bro15conn::bro15conn(std::string const& filename)
  : line(filename)
{
}

option<event> bro15conn::parse(std::string const& line)
{
  VAST_ENTER();

  // TODO: switch to grammar-based parsing.
  using vast::parse;
  event e;
  e.name("bro::conn");
  e.timestamp(now());

  util::field_splitter<std::string::const_iterator> fs;
  fs.split(line.begin(), line.end(), 13);
  if (fs.fields() < 12)
  {
    VAST_LOG_ERROR("less than 12 conn.log fields (line " << current_ << ')');
    return {};
  }

  // Timestamp
  auto i = fs.start(0);
  auto j = fs.end(0);
  time_range range;
  if (! parse(i, j, range) || i != j) 
  {
    VAST_LOG_ERROR(
        "invalid conn.log timestamp (field 1) (line " << current_ << ')');
    return {};
  }
  e.emplace_back(time_point(range));

  // Duration
  i = fs.start(1);
  j = fs.end(1);
  if (*i == '?')
  {
    e.emplace_back(nil);
  }
  else
  {
    time_range range;
    if (! parse(i, j, range) || i != j) 
    {
      VAST_LOG_ERROR(
          "invalid conn.log duration (field 2) (line " << current_ << ')');
      return {};
    }
    e.emplace_back(range);
  }

  // Originator address
  i = fs.start(2);
  j = fs.end(2);
  address addr;
  if (! parse(i, j, addr) || i != j) 
  {
    VAST_LOG_ERROR("invalid conn.log originating address (field 3) (line " <<
                     current_ << ')');
    return {};
  }
  e.emplace_back(std::move(addr));

  // Responder address
  i = fs.start(3);
  j = fs.end(3);
  if (! parse(i, j, addr) || i != j) 
  {
    VAST_LOG_ERROR("invalid conn.log responding address (field 4) (line " <<
                     current_ << ')');
    return {};
  }
  e.emplace_back(std::move(addr));

  // Service
  i = fs.start(4);
  j = fs.end(4);
  if (*i == '?')
  {
    e.emplace_back(nil);
  }
  else
  {
    string service;
    if (! parse(i, j, service) || i != j) 
    {
      VAST_LOG_ERROR("invalid conn.log service (field 5) (line " <<
                       current_ << ')');
      return {};
    }
    e.emplace_back(std::move(service));
  }

  // Ports and protocol
  i = fs.start(5);
  j = fs.end(5);
  port orig_p;
  if (! parse(i, j, orig_p) || i != j) 
  {
    VAST_LOG_ERROR("invalid conn.log originating port (field 6) (line " <<
                     current_ << ')');
    return {};
  }

  i = fs.start(6);
  j = fs.end(6);
  port resp_p;
  if (! parse(i, j, resp_p) || i != j) 
  {
    VAST_LOG_ERROR("invalid conn.log responding port (field 7) (line " <<
                     current_ << ')');
    return {};
  }

  i = fs.start(7);
  j = fs.end(7);
  string proto;
  if (! parse(i, j, proto) || i != j) 
  {
    VAST_LOG_ERROR("invalid conn.log proto (field 8) (line " <<
                     current_ << ')');
    return {};
  }

  auto p = port::unknown;
  if (proto == "tcp")
    p = port::tcp;
  else if (proto == "udp")
    p = port::udp;
  else if (proto == "icmp")
    p = port::icmp;
  orig_p.type(p);
  resp_p.type(p);
  e.emplace_back(std::move(orig_p));
  e.emplace_back(std::move(resp_p));
  e.emplace_back(std::move(proto));

  // Originator and responder bytes
  i = fs.start(8);
  j = fs.end(8);
  if (*i == '?')
  {
    e.emplace_back(nil);
  }
  else
  {
    uint64_t orig_bytes;
    if (! parse(i, j, orig_bytes) || i != j) 
    {
      VAST_LOG_ERROR("invalid conn.log originating bytes (field 9) (line " <<
                     current_ << ')');
      return {};
    }
    e.emplace_back(orig_bytes);
  }

  i = fs.start(8);
  j = fs.end(8);
  if (*i == '?')
  {
    e.emplace_back(nil);
  }
  else
  {
    uint64_t resp_bytes;
    if (! parse(i, j, resp_bytes) || i != j) 
    {
      VAST_LOG_ERROR("invalid conn.log responding bytes (field 10) (line " <<
                     current_ << ')');
      return {};
    }
    e.emplace_back(resp_bytes);
  }

  // Connection state
  i = fs.start(10);
  j = fs.end(10);
  string state;
  if (! parse(i, j, state) || i != j) 
  {
    VAST_LOG_ERROR("invalid conn.log connection state (field 11) (line " <<
                   current_ << ')');
    return {};
  }
  e.emplace_back(std::move(state));

  // Direction
  i = fs.start(11);
  j = fs.end(11);
  string direction;
  if (! parse(i, j, direction) || i != j) 
  {
    VAST_LOG_ERROR("invalid conn.log direction (field 12) (line " <<
                   current_ << ')');
    return {};
  }
  e.emplace_back(std::move(direction));

  // Additional information
  if (fs.fields() == 13)
  {
    i = fs.start(12);
    j = fs.end(12);
    string addl;
    if (! parse(i, j, addl) || i != j) 
    {
      VAST_LOG_ERROR("invalid conn.log additional data (field 13) (line " <<
                     current_ << ')');
      return {};
    }
    e.emplace_back(std::move(addl));
  }

  return e;
}

} // namespace source
} // namespace vast
