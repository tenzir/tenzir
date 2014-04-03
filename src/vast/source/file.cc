#include "vast/source/file.h"

#include "vast/util/field_splitter.h"
#include "vast/schema.h"

namespace vast {
namespace source {

namespace {

// Converts a Bro type to a VAST type.
value_type bro_to_vast(string const& type)
{
  if (type == "enum" || type == "string" || type == "file")
    return string_value;
  else if (type == "bool")
    return bool_value;
  else if (type == "int")
    return int_value;
  else if (type == "count")
    return uint_value;
  else if (type == "double")
    return double_value;
  else if (type == "interval")
    return time_range_value;
  else if (type == "time")
    return time_point_value;
  else if (type == "addr")
    return address_value;
  else if (type == "port")
    return port_value;
  else if (type == "pattern")
    return regex_value;
  else if (type == "subnet")
    return prefix_value;
  else if (type.starts_with("record"))
    return record_value;
  else if (type.starts_with("vector"))
    return vector_value;
  else if (type.starts_with("set"))
    return set_value;
  else if (type.starts_with("table"))
    return table_value;
  else
    return invalid_value;
}

} // namespace <anonymous>

bro2::bro2(cppa::actor_ptr sink, std::string const& filename,
           int32_t timestamp_field)
  : line<bro2>{std::move(sink), filename},
    timestamp_field_{timestamp_field}
{
}

result<event> bro2::extract_impl_impl()
{
  using vast::extract;

  util::field_splitter<std::string::const_iterator>
    fs{separator_.data(), separator_.size()};

  // We assume that each Bro log comes with more than zero field names.
  // Consequently, if we have not yet recorded any field names, we still need
  // to parse the header.
  if (field_names_.empty())
  {
    auto line = this->next();
    if (! line)
      return error{"could not read first line of header"};

    auto t = parse_header();
    if (! t)
      return t.failure();
  }

  auto line = this->next();
  if (! line)
    return {};

  fs.split(line->begin(), line->end());
  if (fs.fields() > 0 && *fs.start(0) == '#')
  {
    auto first = string{fs.start(0), fs.end(0)};
    if (first.starts_with("#separator"))
    {
      VAST_LOG_ACTOR_VERBOSE("restarts with new log");

      timestamp_field_ = -1;
      separator_ = " ";
      field_names_.clear();
      field_types_.clear();
      complex_types_.clear();

      auto t = parse_header();
      if (! t)
        return t.failure();

      auto line = this->next();
      if (! line)
        return {};

      fs = {separator_.data(), separator_.size()};
      fs.split(line->begin(), line->end());
    }
    else
    {
      VAST_LOG_ACTOR_INFO("ignored comment at line " << number() <<
                   ": " << *line);
      return {};
    }
  }

  if (fs.fields() != field_types_.size())
    return error{"inconsistent number of fields at line " +
                 to_string(number()) + ": expected " +
                 to_string(field_types_.size()) + ", got " +
//                 to_string(fs.fields())};
                 to_string(fs.fields()) + " (" + *line + ')'};

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

    if (field_types_[f] == record_value)
    {
      record r;
      if (! extract(start, end, r, complex_types_[containers++],
                    set_separator_, "{", "}"))
        return error{"got invalid record syntax"};

      e.emplace_back(std::move(r));
    }
    else if (field_types_[f] == vector_value)
    {
      vector v;
      if (! extract(start, end, v, complex_types_[containers++],
                    set_separator_, "{", "}"))
        return error{"got invalid vector syntax"};

      e.emplace_back(std::move(v));
    }
    else if (field_types_[f] == set_value || field_types_[f] == table_value)
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

      if (f == static_cast<size_t>(timestamp_field_)
          && v.which() == time_point_value)
        e.timestamp(v.get<time_point>());

      e.push_back(std::move(v));
    }
  }

  return std::move(e);
}

char const* bro2::description_impl_impl() const
{
  return "bro2-source";
}

trial<schema> bro2::parse_header()
{
  auto line = this->current();
  if (! line)
    return error{"failed to retrieve first header line"};

  util::field_splitter<std::string::const_iterator>
    fs{separator_.data(), separator_.size()};

  fs.split(line->begin(), line->end());

  if (fs.fields() != 2 || ! fs.equals(0, "#separator"))
    return error{"got invalid #separator"};

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

  separator_ = string{sep.begin(), sep.end()};
  fs = {separator_.data(), separator_.size()};

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  fs.split(line->begin(), line->end());
  if (fs.fields() != 2 || ! fs.equals(0, "#set_separator"))
    return error{"got invalid #set_separator"};

  auto set_sep = std::string(fs.start(1), fs.end(1));
  set_separator_ = string(set_sep.begin(), set_sep.end());

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  fs.split(line->begin(), line->end());
  if (fs.fields() != 2 || ! fs.equals(0, "#empty_field"))
    return error{"invalid #empty_field"};

  auto empty = std::string(fs.start(1), fs.end(1));
  empty_field_ = string(empty.begin(), empty.end());

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  fs.split(line->begin(), line->end());
  if (fs.fields() != 2 || ! fs.equals(0, "#unset_field"))
    return error{"invalid #unset_field"};

  auto unset = std::string(fs.start(1), fs.end(1));
  unset_field_ = string(unset.begin(), unset.end());

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  fs.split(line->begin(), line->end());
  if (fs.fields() != 2 || ! fs.equals(0, "#path"))
    return error{"invalid #path"};

  path_ = "bro::" + string(fs.start(1), fs.end(1));

  // Skip #open tag.
  line = this->next();
  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  fs.split(line->begin(), line->end());
  if (! fs.equals(0, "#fields"))
    return error{"got invalid #fields"};

  for (size_t i = 1; i < fs.fields(); ++i)
    field_names_.emplace_back(fs.start(i), fs.end(i));

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  fs.split(line->begin(), line->end());
  if (! fs.equals(0, "#types"))
    return error{"got invalid #types"};

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

  VAST_LOG_ACTOR_DEBUG("parsed bro2 header:");
  VAST_LOG_ACTOR_DEBUG("    #separator " << separator_);
  VAST_LOG_ACTOR_DEBUG("    #set_separator " << set_separator_);
  VAST_LOG_ACTOR_DEBUG("    #empty_field " << empty_field_);
  VAST_LOG_ACTOR_DEBUG("    #unset_field " << unset_field_);
  VAST_LOG_ACTOR_DEBUG("    #path " << path_);

  assert(field_names_.size() == field_types_.size());
  VAST_LOG_ACTOR_DEBUG("  fields:");
  for (size_t i = 0; i < field_names_.size(); ++i)
    VAST_LOG_ACTOR_DEBUG("    " << i << ") " << field_names_[i] <<
                         " (" << field_types_[i] << ')');

  //if (! complex_types_.empty())
  //{
  //  VAST_LOG_ACTOR_DEBUG("  complex fields:");
  //  for (size_t i = 0; i < complex_types_.size(); ++i)
  //    VAST_LOG_ACTOR_DEBUG("    " << i << ") " << complex_types_[i]);
  //}

  if (timestamp_field_ > -1)
  {
    VAST_LOG_ACTOR_VERBOSE("attempts to extract timestamp from field " <<
                           timestamp_field_);
  }
  else
  {
    for (size_t i = 0; i < field_types_.size(); ++i)
      if (field_types_[0] == time_point_value)
      {
        VAST_LOG_ACTOR_VERBOSE("auto-detected field " << i <<
                               " as event timestamp");
        timestamp_field_ = static_cast<int32_t>(i);
        break;
      }
  }

  //std::vector<argument> args;
  //for (size_t i = 0; i < field_names_; ++i)
  //{
  //  schema::argument a;
  //  a.name = field_names_[i];

  //  e.args.push_back(std::move(a));
  //}

  //event_info e{path_, ;
  schema s;

  return s;
}

} // namespace source
} // namespace vast
