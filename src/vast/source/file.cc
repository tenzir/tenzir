#include <vast/source/file.h>

#include <ze.h>
#include <ze/parse.h>
#include "vast/util/field_splitter.h"
#include "vast/logger.h"

namespace vast {
namespace source {

file::file(std::string const& filename)
  : file_(filename)
{
  if (file_)
    file_.unsetf(std::ios::skipws);
  else
    LOG(error, ingest)
      << "file source @" << id() << " cannot read " << filename;
}

bool file::finished()
{
  return file_.good();
}

line::line(std::string const& filename)
  : file(filename)
{
  next();
}

option<ze::event> line::extract()
{
  while (line_.empty())
    if (! next())
      break;

  auto e = parse(line_);
  next();
  return e;
}

bool line::next()
{
  auto success = false;
  if (std::getline(file_, line_))
    success = true;
  ++current_;
  return success;
}

bro2::bro2(std::string const& filename)
  : line(filename)
{
  if (! parse_header())
  {
    LOG(error, ingest) << "not a valid bro 2.x log file: " << filename;
    if (file_)
      file_.close();
  }
}

bool bro2::parse_header()
{
  if (line_.empty() || line_[0] != '#')
  {
    LOG(error, ingest)
      << "no meta character '#' at start of first line";
    return false;
  }

  util::field_splitter<std::string::const_iterator> fs;
  fs.split(line_.begin(), line_.end());
  if (fs.fields() != 2 || std::string(fs.start(0), fs.end(0)) != "#separator")
  {
    LOG(error, ingest) << "invalid #separator definition";
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

    separator_ = ze::string(sep.begin(), sep.end());
  }

  {
    if (! next())
    {
      LOG(error, ingest) << "could not extract second log line";
      return false;
    }

    util::field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line_.begin(), line_.end());
    if (fs.fields() != 2 
        || std::string(fs.start(0), fs.end(0)) != "#set_separator")
    {
      LOG(error, ingest) << "invalid #set_separator definition";
      return false;
    }

    auto set_sep = std::string(fs.start(1), fs.end(1));
    set_separator_ = ze::string(set_sep.begin(), set_sep.end());
  }

  {
    if (! next())
    {
      LOG(error, ingest) << "could not extract third log line";
      return false;
    }

    util::field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line_.begin(), line_.end());
    if (fs.fields() != 2 
        || std::string(fs.start(0), fs.end(0)) != "#empty_field")
    {
      LOG(error, ingest) << "invalid #empty_field definition";
      return false;
    }

    auto empty = std::string(fs.start(1), fs.end(1));
    empty_field_ = ze::string(empty.begin(), empty.end());
  }

  {
    if (! next())
    {
      LOG(error, ingest) << "could not extract fourth log line";
      return false;
    }

    util::field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line_.begin(), line_.end());
    if (fs.fields() != 2 || std::string(fs.start(0), fs.end(0)) != "#unset_field")
    {
      LOG(error, ingest) << "invalid #unset_field definition";
      return false;
    }

    auto unset = std::string(fs.start(1), fs.end(1));
    unset_field_ = ze::string(unset.begin(), unset.end());
  }

  {
    if (! next())
    {
      LOG(error, ingest) << "could not extract fifth log line";
      return false;
    }

    util::field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line_.begin(), line_.end());
    if (fs.fields() != 2 || std::string(fs.start(0), fs.end(0)) != "#path")
    {
      LOG(error, ingest) << "invalid #path definition";
      return false;
    }

    path_ = ze::string(fs.start(1), fs.end(1));
  }

  {
    if (! next())
    {
      LOG(error, ingest) << "could not extract sixth log line";
      return false;
    }

    util::field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line_.begin(), line_.end());

    for (size_t i = 1; i < fs.fields(); ++i)
      field_names_.emplace_back(fs.start(i), fs.end(i));
  }

  {
    if (! next())
    {
      LOG(error, ingest) << "could not extract seventh log line";
      return false;
    }

    util::field_splitter<std::string::const_iterator> fs;
    fs.sep(separator_.data(), separator_.size());
    fs.split(line_.begin(), line_.end());

    for (size_t i = 1; i < fs.fields(); ++i)
    {
      ze::string t(fs.start(i), fs.end(i));
      if (t.starts_with("table") || t.starts_with("vector"))
      {
        field_types_.push_back(ze::set_type);
        auto open = t.find("[");
        assert(open != ze::string::npos);
        auto close = t.find("]", open);
        assert(close != ze::string::npos);
        auto elem = t.substr(open + 1, close - open - 1);
        set_types_.push_back(bro_to_ze(elem));
      }
      else
      {
        field_types_.push_back(bro_to_ze(t));
      }
    }
  }

  if (file_.peek() == '#')
  {
    LOG(error, ingest) << "more headers than VAST knows";
    return false;
  }

  DBG(ingest)
    << "event source @" << id() << " parsed bro2 header:"
    << " #separator " << separator_
    << " #set_separator " << set_separator_
    << " #empty_field " << empty_field_
    << " #unset_field " << unset_field_
    << " #path " << path_;
  {
    std::ostringstream str;
    for (auto& name : field_names_)
      str << " " << name;
    DBG(ingest) << "event source @" << id() << " has field names:" << str.str();
  }
  {
    std::ostringstream str;
    for (auto& type : field_types_)
      str << " " << type;
    DBG(ingest) << "event source @" << id() << " has field types:" << str.str();
  }
  if (! set_types_.empty())
  {
    std::ostringstream str;
    for (auto& type : set_types_)
      str << " " << type;
    DBG(ingest) << "event source @" << id() << " has set types:" << str.str();
  }

  path_ = "bro::" + path_;
  next();

  return true;
}

ze::value_type bro2::bro_to_ze(ze::string const& type)
{
  if (type == "enum" || type == "string" || type == "file")
    return ze::string_type;
  else if (type == "bool")
    return ze::bool_type;
  else if (type == "int")
    return ze::int_type;
  else if (type == "count")
    return ze::uint_type;
  else if (type == "double")
    return ze::double_type;
  else if (type == "interval")
    return ze::time_range_type;
  else if (type == "time")
    return ze::time_point_type;
  else if (type == "addr")
    return ze::address_type;
  else if (type == "port")
    return ze::port_type;
  else if (type == "pattern")
    return ze::regex_type;
  else if (type == "subnet")
    return ze::prefix_type;
  else
    return ze::invalid_type;
}

option<ze::event> bro2::parse(std::string const& line)
{
  util::field_splitter<std::string::const_iterator> fs;
  fs.sep(separator_.data(), separator_.size());
  fs.split(line.begin(), line.end());
  if (fs.fields() != field_types_.size())
  {
    LOG(error, ingest) 
      << "inconsistent number of fields (line " << current_ << ')';
    return {};
  }

  ze::event e;
  e.name(path_);
  e.timestamp(ze::now());
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
      e.push_back(ze::nil);
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
    if (field_types_[f] == ze::set_type)
    {
      ze::set s;
      // TODO: take care of escaped set separators.
      auto success = ze::parse(start, end, s, set_types_[sets++], set_separator_);
      if (! success)
      {
        LOG(error, ingest) << "invalid set syntax";
        return {};
      }
      e.emplace_back(std::move(s));
    }
    else
    {
      ze::value v;
      auto success = ze::parse(start, end, v, field_types_[f]);
      if (! success)
      {
        LOG(error, ingest) << "could not parse field";
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

option<ze::event> bro15conn::parse(std::string const& line)
{
  ze::event e;
  e.name("bro::conn");
  e.timestamp(ze::now());

  util::field_splitter<std::string::const_iterator> fs;
  fs.split(line.begin(), line.end(), 13);
  if (fs.fields() < 12)
  {
    LOG(error, ingest)
      << "less than 12 conn.log fields (line " << current_ << ')';
    return {};
  }

  // Timestamp
  auto i = fs.start(0);
  auto j = fs.end(0);
  ze::time_range range;
  if (! ze::parse(i, j, range) || i != j) 
  {
    LOG(error, ingest)
      << "invalid conn.log timestamp (field 1) (line " << current_ << ')';
    return {};
  }
  e.emplace_back(ze::time_point(range));

  // Duration
  i = fs.start(1);
  j = fs.end(1);
  if (*i == '?')
  {
    e.emplace_back(ze::nil);
  }
  else
  {
    ze::time_range range;
    if (! ze::parse(i, j, range) || i != j) 
    {
      LOG(error, ingest)
        << "invalid conn.log duration (field 2) (line " << current_ << ')';
      return {};
    }
    e.emplace_back(range);
  }

  // Originator address
  i = fs.start(2);
  j = fs.end(2);
  ze::address addr;
  if (! ze::parse(i, j, addr) || i != j) 
  {
    LOG(error, ingest)
      << "invalid conn.log originating address (field 3) (line "
      << current_ << ')';
    return {};
  }
  e.emplace_back(std::move(addr));

  // Responder address
  i = fs.start(3);
  j = fs.end(3);
  if (! ze::parse(i, j, addr) || i != j) 
  {
    LOG(error, ingest)
      << "invalid conn.log responding address (field 4) (line "
      << current_ << ')';
    return {};
  }
  e.emplace_back(std::move(addr));

  // Service
  i = fs.start(4);
  j = fs.end(4);
  if (*i == '?')
  {
    e.emplace_back(ze::nil);
  }
  else
  {
    ze::string service;
    if (! ze::parse(i, j, service) || i != j) 
    {
      LOG(error, ingest)
        << "invalid conn.log service (field 5) (line " << current_ << ')';
      return {};
    }
    e.emplace_back(std::move(service));
  }

  // Ports and protocol
  i = fs.start(5);
  j = fs.end(5);
  ze::port orig_p;
  if (! ze::parse(i, j, orig_p) || i != j) 
  {
    LOG(error, ingest)
      << "invalid conn.log originating port (field 6) (line "
      << current_ << ')';
    return {};
  }

  i = fs.start(6);
  j = fs.end(6);
  ze::port resp_p;
  if (! ze::parse(i, j, resp_p) || i != j) 
  {
    LOG(error, ingest)
      << "invalid conn.log responding port (field 7) (line "
      << current_ << ')';
    return {};
  }

  i = fs.start(7);
  j = fs.end(7);
  ze::string proto;
  if (! ze::parse(i, j, proto) || i != j) 
  {
    LOG(error, ingest)
      << "invalid conn.log proto (field 8) (line " << current_ << ')';
    return {};
  }

  auto p = ze::port::unknown;
  if (proto == "tcp")
    p = ze::port::tcp;
  else if (proto == "udp")
    p = ze::port::udp;
  else if (proto == "icmp")
    p = ze::port::icmp;
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
    e.emplace_back(ze::nil);
  }
  else
  {
    uint64_t orig_bytes;
    if (! ze::parse(i, j, orig_bytes) || i != j) 
    {
      LOG(error, ingest)
        << "invalid conn.log originating bytes (field 9) (line "
        << current_ << ')';
      return {};
    }
    e.emplace_back(orig_bytes);
  }

  i = fs.start(8);
  j = fs.end(8);
  if (*i == '?')
  {
    e.emplace_back(ze::nil);
  }
  else
  {
    uint64_t resp_bytes;
    if (! ze::parse(i, j, resp_bytes) || i != j) 
    {
      LOG(error, ingest)
        << "invalid conn.log responding bytes (field 10) (line "
        << current_ << ')';
      return {};
    }
    e.emplace_back(resp_bytes);
  }

  // Connection state
  i = fs.start(10);
  j = fs.end(10);
  ze::string state;
  if (! ze::parse(i, j, state) || i != j) 
  {
    LOG(error, ingest)
      << "invalid conn.log connection state (field 11) (line "
      << current_ << ')';
    return {};
  }
  e.emplace_back(std::move(state));

  // Direction
  i = fs.start(11);
  j = fs.end(11);
  ze::string direction;
  if (! ze::parse(i, j, direction) || i != j) 
  {
    LOG(error, ingest)
      << "invalid conn.log direction (field 12) (line " << current_ << ')';
    return {};
  }
  e.emplace_back(std::move(direction));

  // Additional information
  if (fs.fields() == 13)
  {
    i = fs.start(12);
    j = fs.end(12);
    ze::string addl;
    if (! ze::parse(i, j, addl) || i != j) 
    {
      LOG(error, ingest)
        << "invalid conn.log additional data (field 13) (line "
        << current_ << ')';
      return {};
    }
    e.emplace_back(std::move(addl));
  }

  return e;
}

} // namespace source
} // namespace vast
