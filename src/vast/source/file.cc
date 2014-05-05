#include "vast/source/file.h"

#include "vast/util/field_splitter.h"

namespace vast {
namespace source {

namespace {

trial<type_const_ptr> make_type(schema& sch, string const& bro_type)
{
  type_ptr t;
  if (bro_type == "enum" || bro_type == "string" || bro_type == "file")
    t = type::make<string_type>();
  else if (bro_type == "bool")
    t = type::make<bool_type>();
  else if (bro_type == "int")
    t = type::make<int_type>();
  else if (bro_type == "count")
    t = type::make<uint_type>();
  else if (bro_type == "double")
    t = type::make<double_type>();
  else if (bro_type == "interval")
    t = type::make<time_range_type>();
  else if (bro_type == "time")
    t = type::make<time_point_type>();
  else if (bro_type == "pattern")
    t = type::make<regex_type>();
  else if (bro_type == "addr")
    t = type::make<address_type>();
  else if (bro_type == "subnet")
    t = type::make<prefix_type>();
  else if (bro_type == "port")
    t = type::make<port_type>();

  if (! t && (bro_type.starts_with("vector")
              || bro_type.starts_with("set")
              || bro_type.starts_with("table")))
  {
    auto open = bro_type.find("[");
    auto close = bro_type.find("]", open);
    if (open == string::npos || close == string::npos)
      return error{"invalid element type"};

    auto elem = bro_type.substr(open + 1, close - open - 1);
    auto elem_type = make_type(sch, elem);
    if (! elem_type)
      return elem_type.error();

    // Bro cannot log nested vectors/sets/tables, so we can safely assume that
    // we're dealing with a basic type here.
    if (bro_type.starts_with("vector"))
      t = type::make<vector_type>("", *elem_type);
    else
      t = type::make<set_type>("", *elem_type);
  }

  if (t)
  {
    auto types = sch.find_type_info(t->info());
    if (types.empty())
    {
      auto status = sch.add(t);
      if (t)
        return {t};
      else
        return error{"failed to add type to schema: " + status.error().msg()};
    }

    if (types.size() != 1)
      return error{"schema inconsistency: at most one type possible"};

    return types[0];
  }

  return error{"failed to make type for: " + to_string(bro_type)};
}

} // namespace <anonymous>

bro2::bro2(cppa::actor_ptr sink, std::string const& filename,
           int32_t timestamp_field)
  : file<bro2>{std::move(sink), filename},
    timestamp_field_{timestamp_field}
{
}

result<event> bro2::extract_impl()
{
  using vast::extract;

  util::field_splitter<std::string::const_iterator>
    fs{separator_.data(), separator_.size()};

  if (! type_)
  {
    auto line = this->next();
    if (! line)
      return error{"could not read first line of header"};

    auto t = parse_header();
    if (! t)
      return t.error();
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

      auto t = parse_header();
      if (! t)
        return t.error();

      auto line = this->next();
      if (! line)
        return {};

      fs = {separator_.data(), separator_.size()};
      fs.split(line->begin(), line->end());
    }
    else
    {
      VAST_LOG_ACTOR_VERBOSE("ignored comment at line " << line_number() <<
                             ": " << *line);
      return {};
    }
  }

  record rec;
  auto ts = now();

  for (size_t f = 0; f < fs.fields(); ++f)
  {
    auto& args = util::get<record_type>(flat_type_->info())->args;
    assert(f < args.size());

    auto start = fs.start(f);
    auto end = fs.end(f);
    auto& type = args[f].type;

    // Check whether the field is set. Default: '-'
    auto unset = true;
    for (size_t i = 0; i < unset_field_.size(); ++i)
      if (unset_field_[i] != *(start + i))
      {
        unset = false;
        break;
      }

    if (unset)
    {
      rec.emplace_back(type->tag());
      continue;
    }

    // Check whether the field is empty. Default: "(empty)"
    auto empty = true;
    for (size_t i = 0; i < empty_field_.size(); ++i)
      if (empty_field_[i] != *(start + i))
      {
        empty = false;
        break;
      }

    if (empty)
    {
      rec.emplace_back(type->tag());
      continue;
    }

    value v;
    auto success = extract(start, end, v, type,
                           set_separator_, "", "",
                           set_separator_, "{", "}");
    if (! success)
      return error{"could not parse field: " + std::string{start, end}};

    if (f == size_t(timestamp_field_) && v.which() == time_point_value)
      ts = v.get<time_point>();

    rec.push_back(std::move(v));
  }

  // Unflatten the record.
  event e;
  e.type(type_);
  e.timestamp(ts);
  size_t f = 0;
  size_t d = 1;
  record* r = &e;
  type_->each(
      [&](key const&, offset const& o)
      {
        if (o.size() > d)
        {
          ++d;
          e.emplace_back(record{});
          r = &e.back().get<record>();
        }
        else if (o.size() < d)
        {
          --d;
          r = &e;
          for (size_t i = 0; i < d - 1; ++i)
            r = &(*r)[i].get<record>();
        }

        r->push_back(std::move(rec[f++]));
      });

  return std::move(e);
}

char const* bro2::description_impl() const
{
  return "bro2-source";
}

trial<void> bro2::parse_header()
{
  auto line = this->current_line();
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

  auto event_name = string{fs.start(1), fs.end(1)};

  line = this->next(); // Skip #open tag.
  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  fs.split(line->begin(), line->end());
  if (! fs.equals(0, "#fields"))
    return error{"got invalid #fields"};

  std::vector<string> arg_names;
  for (size_t i = 1; i < fs.fields(); ++i)
    arg_names.emplace_back(fs.start(i), fs.end(i));

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  fs.split(line->begin(), line->end());
  if (! fs.equals(0, "#types"))
    return error{"got invalid #types"};

  record_type event_record;
  for (size_t i = 1; i < fs.fields(); ++i)
  {
    string bro_type = {fs.start(i), fs.end(i)};
    auto t = make_type(schema_, bro_type);
    if (! t)
      return t.error();

    event_record.args.emplace_back(arg_names[i - 1], *t);
  }

  type_ = type::make<record_type>(event_name, event_record.unflatten());
  flat_type_ = type::make<record_type>(event_name, std::move(event_record));
  auto status = schema_.add(type_);
  if (! status)
    return error{"failed to add type to schema: " + status.error().msg()};

  VAST_LOG_ACTOR_DEBUG("parsed bro2 header:");
  VAST_LOG_ACTOR_DEBUG("    #separator " << separator_);
  VAST_LOG_ACTOR_DEBUG("    #set_separator " << set_separator_);
  VAST_LOG_ACTOR_DEBUG("    #empty_field " << empty_field_);
  VAST_LOG_ACTOR_DEBUG("    #unset_field " << unset_field_);
  VAST_LOG_ACTOR_DEBUG("    #path " << type_->name());

  auto rec = util::get<record_type>(flat_type_->info());

  VAST_LOG_ACTOR_DEBUG("    fields:");
  for (size_t i = 0; i < rec->args.size(); ++i)
    VAST_LOG_ACTOR_DEBUG("      " << i << ") " << rec->args[i]);

  if (timestamp_field_ > -1)
  {
    VAST_LOG_ACTOR_VERBOSE("attempts to extract timestamp from field " <<
                           timestamp_field_);
  }
  else
  {
    size_t i = 0;
    for (auto& arg : rec->args)
    {
      if (util::get<time_point_type>(arg.type->info()))
      {
        VAST_LOG_ACTOR_VERBOSE("auto-detected field " << i <<
                               " as event timestamp");
        timestamp_field_ = static_cast<int32_t>(i);
        break;
      }

      ++i;
    }
  }

  return nothing;
}

} // namespace source
} // namespace vast
