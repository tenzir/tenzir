#include "vast/source/file.h"

#include "vast/io/getline.h"
#include "vast/util/field_splitter.h"

namespace vast {
namespace source {

file::file(cppa::actor_ptr sink, std::string const& filename)
  : synchronous(sink),
    file_handle_(path(filename)),
    file_stream_(file_handle_)
{
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

result<event> line::extract()
{
  VAST_ENTER();

  result<event> e;
  if (! line_.empty())
    e = std::move(parse(line_));

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
  if (! parse_header())
  {
    VAST_LOG_ACTOR_ERROR("cannot parse Bro 2.x log file header");
    finished_ = true;
  }
}

char const* bro2::description() const
{
  return "bro2-source";
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
    VAST_LOG_ACTOR_ERROR("got invalid #separator");
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
      VAST_LOG_ACTOR_ERROR("got invalid #set_separator");
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
      VAST_LOG_ACTOR_ERROR("invalid #empty_field");
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
      VAST_LOG_ACTOR_ERROR("invalid #unset_field");
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
      VAST_LOG_ACTOR_ERROR("invalid #path");
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
      VAST_LOG_ACTOR_ERROR("got invalid #fields");
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
      VAST_LOG_ACTOR_ERROR("got invalid #types");
      return false;
    }

    for (size_t i = 1; i < fs.fields(); ++i)
    {
      string t(fs.start(i), fs.end(i));
      auto type = bro_to_vast(t);
      field_types_.push_back(type);
      if (is_container_type(type))
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

  VAST_LOG_ACTOR_DEBUG("parsed bro2 header:" <<
                       " #separator " << separator_ <<
                       " #set_separator " << set_separator_ <<
                       " #empty_field " << empty_field_ <<
                       " #unset_field " << unset_field_ <<
                       " #path " << path_);
  {
    std::ostringstream str;
    for (auto& name : field_names_)
      str << " " << name;
    VAST_LOG_ACTOR_DEBUG("has field names:" << str.str());
  }
  {
    std::ostringstream str;
    for (auto& type : field_types_)
      str << " " << type;
    VAST_LOG_ACTOR_DEBUG("has field types:" << str.str());
  }
  if (! complex_types_.empty())
  {
    std::ostringstream str;
    for (auto& type : complex_types_)
      str << " " << type;
    VAST_LOG_ACTOR_DEBUG("has container types:" << str.str());
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
  else if (type.starts_with("record"))
    return record_type;
  else if (type.starts_with("vector"))
    return vector_type;
  else if (type.starts_with("set"))
    return set_type;
  else if (type.starts_with("table"))
    return table_type;
  else
    return invalid_type;
}

result<event> bro2::parse(std::string const& line)
{
  using vast::extract;
  VAST_ENTER();

  // TODO: switch to grammar-based parsing.
  util::field_splitter<std::string::const_iterator>
    fs{separator_.data(), separator_.size()};

  fs.split(line.begin(), line.end());

  if (fs.fields() > 0 && *fs.start(0) == '#')
    return error{"ignored comment: " + line +
                 " (line " + to_string(current_) + ')'};

  if (fs.fields() != field_types_.size())
    return error{"inconsistent number of fields"
                 " (line " + to_string(current_) + ')'};

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
        return error{"got invalid record syntax"};

      e.emplace_back(std::move(r));
    }
    else if (field_types_[f] == vector_type)
    {
      vector v;
      if (! extract(start, end, v, complex_types_[containers++],
                    set_separator_, "{", "}"))
        return error{"got invalid vector syntax"};

      e.emplace_back(std::move(v));
    }
    else if (field_types_[f] == set_type || field_types_[f] == table_type)
    {
      set s;
      if (! extract(start, end, s, complex_types_[containers++],
                    set_separator_, "{", "}"))
        return error{"got invalid set/table syntax"};

      e.emplace_back(std::move(s));
    }
    else
    {
      value v;
      if (! extract(start, end, v, field_types_[f]))
        return error{"could not parse field: " + std::string{start, end}};

      e.push_back(std::move(v));
    }
  }

  return std::move(e);
}

} // namespace source
} // namespace vast
