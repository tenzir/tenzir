#include "vast/source/file.h"

#include "vast/logger.h"
#include "vast/io/getline.h"
#include "vast/util/field_splitter.h"

namespace vast {
namespace source {

file::file(cppa::actor_ptr sink, std::string const& filename)
  : synchronous(sink),
    file_handle_(path(filename)),
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

line::line(cppa::actor_ptr sink, std::string const& filename)
  : file(sink, filename)
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

bro2::bro2(cppa::actor_ptr sink, std::string const& filename)
  : line(sink, filename)
{
  VAST_LOG_ACT_VERBOSE("bro2-source", "spawned for " << filename);

  if (! parse_header())
  {
    VAST_LOG_ACT_ERROR("bro2-source", "cannot parse Bro 2.x log file header");
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
    VAST_LOG_ACT_ERROR("bro2-source", "got invalid #separator");
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
      VAST_LOG_ACT_ERROR("bro2-source", "got invalid #set_separator");
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
      VAST_LOG_ACT_ERROR("bro2-source", "invalid #empty_field");
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
      VAST_LOG_ACT_ERROR("bro2-source", "invalid #unset_field");
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
      VAST_LOG_ACT_ERROR("bro2-source", "invalid #path");
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
      VAST_LOG_ACT_ERROR("bro2-source", "got invalid #fields");
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
      VAST_LOG_ACT_ERROR("bro2-source", "got invalid #types");
      return false;
    }

    for (size_t i = 1; i < fs.fields(); ++i)
    {
      string t(fs.start(i), fs.end(i));
      auto type = bro_to_vast(t);
      field_types_.push_back(type);
      if (type == record_type || type == table_type)
      {
        auto open = t.find("[");
        assert(open != string::npos);
        auto close = t.find("]", open);
        assert(close != string::npos);
        auto elem = t.substr(open + 1, close - open - 1);
        complex_types_.push_back(bro_to_vast(elem));
      }
    }
  }

  line_.clear(); // Triggers call to next().

  VAST_LOG_ACT_DEBUG("bro2-source", "parsed bro2 header:" <<
                     " #separator " << separator_ <<
                     " #set_separator " << set_separator_ <<
                     " #empty_field " << empty_field_ <<
                     " #unset_field " << unset_field_ <<
                     " #path " << path_);
  {
    std::ostringstream str;
    for (auto& name : field_names_)
      str << " " << name;
    VAST_LOG_ACT_DEBUG("bro2-source", "has field names:" << str.str());
  }
  {
    std::ostringstream str;
    for (auto& type : field_types_)
      str << " " << type;
    VAST_LOG_ACT_DEBUG("bro2-source", "has field types:" << str.str());
  }
  if (! complex_types_.empty())
  {
    std::ostringstream str;
    for (auto& type : complex_types_)
      str << " " << type;
    VAST_LOG_ACT_DEBUG("bro2-source", "has set types:" << str.str());
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
  else if (type.starts_with("set") || type.starts_with("vector"))
    return record_type;
  else if (type.starts_with("table"))
    return table_type;
  else
    return invalid_type;
}

option<event> bro2::parse(std::string const& line)
{
  using vast::extract;
  using vast::parse;
  VAST_ENTER();

  // TODO: switch to grammar-based parsing.
  util::field_splitter<std::string::const_iterator>
    fs{separator_.data(), separator_.size()};

  fs.split(line.begin(), line.end());

  if (fs.fields() > 0 && *fs.start(0) == '#')
  {
    VAST_LOG_ACT_VERBOSE("bro2-source", "ignored comment: " << line <<
                         " (line " << current_ << ')');
    return {};
  }

  if (fs.fields() != field_types_.size())
  {
    VAST_LOG_ACT_ERROR("bro2-source",
                       "found inconsistent number of fields (line " << current_
                       << ')');
    return {};
  }

  event e;
  e.name(path_);
  e.timestamp(now());
  size_t containers = 0;
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
      e.emplace_back(field_types_[f]);
      continue;
    }

    // Check whether the field is empty. (Not "(empty)" by default.)
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

    if (field_types_[f] == record_type)
    {
      record r;
      if (! extract(start, end, r, complex_types_[containers++],
                    set_separator_, "{", "}"))
      {
        // TODO: take care of escaped set separators.
        VAST_LOG_ACT_ERROR("bro2-source", "got invalid set syntax");
        return {};
      }
      e.emplace_back(std::move(r));
    }
    else
    {
      value v;
      if (! extract(start, end, v, field_types_[f]))
      {
        VAST_LOG_ACT_ERROR("bro2-source", "could not parse field: " <<
                           std::string(start, end));
        return {};
      }
      e.push_back(std::move(v));
    }
  }

  return e;
}

bro15conn::bro15conn(cppa::actor_ptr sink, std::string const& filename)
  : line(sink, filename)
{
  VAST_LOG_ACT_VERBOSE("bro15conn",
                       "spawned with conn.log: " << filename << ')');
}

option<event> bro15conn::parse(std::string const& line)
{
  using vast::extract;
  VAST_ENTER();

  // TODO: switch to grammar-based parsing.
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
  if (! extract(i, j, range) || i != j)
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
    e.emplace_back(value{time_range_type});
  }
  else
  {
    time_range range;
    if (! extract(i, j, range) || i != j)
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
  if (! extract(i, j, addr) || i != j)
  {
    VAST_LOG_ERROR("invalid conn.log originating address (field 3) (line " <<
                     current_ << ')');
    return {};
  }
  e.emplace_back(std::move(addr));

  // Responder address
  i = fs.start(3);
  j = fs.end(3);
  if (! extract(i, j, addr) || i != j)
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
    e.emplace_back(value{string_type});
  }
  else
  {
    string service;
    if (! extract(i, j, service) || i != j)
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
  if (! extract(i, j, orig_p) || i != j)
  {
    VAST_LOG_ERROR("invalid conn.log originating port (field 6) (line " <<
                     current_ << ')');
    return {};
  }

  i = fs.start(6);
  j = fs.end(6);
  port resp_p;
  if (! extract(i, j, resp_p) || i != j)
  {
    VAST_LOG_ERROR("invalid conn.log responding port (field 7) (line " <<
                     current_ << ')');
    return {};
  }

  i = fs.start(7);
  j = fs.end(7);
  string proto;
  if (! extract(i, j, proto) || i != j)
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
    e.emplace_back(value{uint_type});
  }
  else
  {
    uint64_t orig_bytes;
    if (! extract(i, j, orig_bytes) || i != j)
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
    e.emplace_back(value{uint_type});
  }
  else
  {
    uint64_t resp_bytes;
    if (! extract(i, j, resp_bytes) || i != j)
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
  if (! extract(i, j, state) || i != j)
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
  if (! extract(i, j, direction) || i != j)
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
    if (! extract(i, j, addl) || i != j)
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
