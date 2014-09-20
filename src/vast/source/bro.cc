#include "vast/source/bro.h"

#include <cassert>
#include "vast/util/string.h"

namespace vast {
namespace source {

namespace {

trial<type> make_type(std::string const& bro_type)
{
  type t;
  if (bro_type == "enum" || bro_type == "string" || bro_type == "file")
    t = type::string{};
  else if (bro_type == "bool")
    t = type::boolean{};
  else if (bro_type == "int")
    t = type::integer{};
  else if (bro_type == "count")
    t = type::count{};
  else if (bro_type == "double")
    t = type::real{};
  else if (bro_type == "time")
    t = type::time_point{};
  else if (bro_type == "interval")
    t = type::time_duration{};
  else if (bro_type == "pattern")
    t = type::pattern{};
  else if (bro_type == "addr")
    t = type::address{};
  else if (bro_type == "subnet")
    t = type::subnet{};
  else if (bro_type == "port")
    t = type::port{};

  if (is<none>(t)
      && (util::starts_with(bro_type, "vector")
          || util::starts_with(bro_type, "set")
          || util::starts_with(bro_type, "table")))
  {
    // Bro's logging framwork cannot log nested vectors/sets/tables, so we can
    // safely assume that we're dealing with a basic type inside the brackets.
    // If this will ever change, we'll have to enhance this simple parser.
    auto open = bro_type.find("[");
    auto close = bro_type.rfind("]");
    if (open == std::string::npos || close == std::string::npos)
      return error{"missing delimiting container brackets: ", bro_type};

    auto elem = make_type(bro_type.substr(open + 1, close - open - 1));
    if (! elem)
      return elem.error();

    // Bro sometimes logs sets as tables, e.g., represents set[string] as
    // table[string]. We iron out this inconsistency by normalizing the type to
    // a set.
    if (util::starts_with(bro_type, "vector"))
      t = type::vector{*elem};
    else
      t = type::set{*elem};
  }

  if (is<none>(t))
    return error{"failed to make type for: ", bro_type};

  return t;
}

} // namespace <anonymous>

bro::bro(schema sch, std::string const& filename, bool sniff)
  : file<bro>{filename},
    schema_{std::move(sch)},
    sniff_{sniff}
{
}

result<event> bro::extract_impl()
{
  if (is<none>(type_))
  {
    auto line = this->next();
    if (! line)
      return error{"could not read first line of header"};

    auto t = parse_header();
    if (! t)
      return t.error();

    if (sniff_)
    {
      schema sch;
      sch.add(type_);
      std::cout << sch << std::flush;
      halt();
      return {};
    }
  }

  auto line = this->next();
  if (! line)
    return {};

  auto s = util::split(*line, separator_);

  if (s.size() > 0 && s[0].first != s[0].second && *s[0].first == '#')
  {
    if (util::starts_with(s[0].first, s[0].second, "#separator"))
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

      s = util::split(*line, separator_);
    }
    else
    {
      VAST_LOG_ACTOR_VERBOSE("ignored comment at line " << line_number() <<
                             ": " << *line);
      return {};
    }
  }

  size_t f = 0;
  size_t depth = 1;
  record event_record;
  record* r = &event_record;
  auto ts = now();
  auto attempt = get<type::record>(type_)->each_field(
      [&](type::record::trace const& t) -> trial<void>
      {
        if (f == s.size())
          return error{"accessed field", f, "out of bounds"};

        if (t.size() > depth)
        {
          for (size_t i = 0; i < t.size() - depth; ++i)
          {
            ++depth;
            r->push_back(record{});
            r = get<record>(r->back());
          }
        }
        else if (t.size() < depth)
        {
          r = &event_record;
          depth = t.size();
          for (size_t i = 0; i < t.size() - 1; ++i)
            r = get<record>(r->back());
        }


        if (std::equal(unset_field_.begin(), unset_field_.end(),
                       s[f].first, s[f].second))
        {
          r->emplace_back(nil);
        }
        else if (std::equal(empty_field_.begin(), empty_field_.end(),
                            s[f].first, s[f].second))
        {
          switch (which(t.back()->type))
          {
            default:
              return error{"invalid empty field ", f, '"', t.back()->name, '"',
                           " of type ", t.back()->type, ": ",
                           std::string{s[f].first, s[f].second}};
            case type::tag::string:
              r->emplace_back(std::string{});
              break;
            case type::tag::vector:
              r->emplace_back(vector{});
              break;
            case type::tag::set:
              r->emplace_back(set{});
              break;
            case type::tag::table:
              r->emplace_back(table{});
              break;
          }
        }
        else
        {
          auto d = parse<data>(s[f].first, s[f].second, t.back()->type,
                               set_separator_, "", "",
                               set_separator_, "", "");
          if (! d)
            return d.error() + error{std::string{s[f].first, s[f].second}};

          if (f == size_t(timestamp_field_))
            if (auto tp = get<time_point>(*d))
              ts = *tp;

          r->push_back(std::move(*d));
        }

        ++f;

        return nothing;
      });

  if (! attempt)
    return attempt.error();

  event e{{std::move(event_record), type_}};
  e.timestamp(ts);

  return std::move(e);
}

std::string bro::describe() const
{
  return "bro-source";
}

trial<std::string> bro::parse_header_line(std::string const& line,
                                          std::string const& prefix)
{
  auto s = util::split(line, separator_, "", 1);

  if (! (s.size() == 2
         && std::equal(prefix.begin(), prefix.end(), s[0].first, s[0].second)))
    return error{"got invalid header line: " + line};


  return std::string{s[1].first, s[1].second};
}

trial<void> bro::parse_header()
{
  auto line = this->current_line();
  if (! line)
    return error{"failed to retrieve first header line"};

  static std::string const separator = "#separator";
  auto header_value = parse_header_line(*line, separator);
  if (! header_value)
    return header_value.error();

  std::string sep;
  std::string::size_type pos = 0;
  while (pos != std::string::npos)
  {
    pos = header_value->find("\\x", pos);
    if (pos != std::string::npos)
    {
      auto i = std::stoi(header_value->substr(pos + 2, 2), nullptr, 16);
      assert(i >= 0 && i <= 255);
      sep.push_back(i);
      pos += 2;
    }
  }

  separator_ = {sep.begin(), sep.end()};

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  static std::string const set_separator = "#set_separator";
  header_value = parse_header_line(*line, set_separator);
  if (! header_value)
    return header_value.error();

  set_separator_ = std::move(*header_value);

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  static std::string const empty_field = "#empty_field";
  header_value = parse_header_line(*line, empty_field);
  if (! header_value)
    return header_value.error();

  empty_field_ = std::move(*header_value);

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  static std::string const unset_field = "#unset_field";
  header_value = parse_header_line(*line, unset_field);
  if (! header_value)
    return header_value.error();

  unset_field_ = std::move(*header_value);

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  static std::string const bro_path = "#path";
  header_value = parse_header_line(*line, bro_path);
  if (! header_value)
    return header_value.error();

  auto event_name = std::move(*header_value);

  line = this->next(); // Skip #open tag.
  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  static std::string const bro_fields = "#fields";
  header_value = parse_header_line(*line, bro_fields);
  if (! header_value)
    return header_value.error();

  auto field_names = util::to_strings(util::split(*header_value, separator_));

  line = this->next();
  if (! line)
    return error{"failed to retrieve next header line"};

  static std::string const bro_types = "#types";
  header_value = parse_header_line(*line, bro_types);
  if (! header_value)
    return header_value.error();

  auto field_types = util::to_strings(util::split(*header_value, separator_));

  if (field_types.size() != field_names.size())
    return error{"differing size of field names and field types"};

  std::vector<type::record::field> fields;
  for (size_t i = 0; i < field_types.size(); ++i)
  {
    auto t = make_type(field_types[i]);
    if (! t)
      return t.error();

    fields.emplace_back(field_names[i], *t);
  }

  type::record flat{std::move(fields)};
  type_ = flat.unflatten();
  type_.name(event_name);

  VAST_LOG_ACTOR_DEBUG("parsed bro header:");
  VAST_LOG_ACTOR_DEBUG("    #separator " << separator_);
  VAST_LOG_ACTOR_DEBUG("    #set_separator " << set_separator_);
  VAST_LOG_ACTOR_DEBUG("    #empty_field " << empty_field_);
  VAST_LOG_ACTOR_DEBUG("    #unset_field " << unset_field_);
  VAST_LOG_ACTOR_DEBUG("    #path " << event_name);
  VAST_LOG_ACTOR_DEBUG("    #fields:");
  for (size_t i = 0; i < flat.fields().size(); ++i)
    VAST_LOG_ACTOR_DEBUG("      " << i << ") " << flat.fields()[i]);

  // If a congruent type exists in the schema, we give the schema type
  // precedence because it may have user-annotated extra information.
  if (auto t = schema_.find_type(event_name))
    if (t->name() == event_name)
    {
      if (congruent(type_, *t))
      {
        VAST_LOG_ACTOR_VERBOSE("prefers type in schema over type in header");
        type_ = *t;
      }
      else
      {
        VAST_LOG_ACTOR_WARN("ignores incongruent types in schema and log: " <<
                            t->name());
      }
    }

  if (timestamp_field_ > -1)
  {
    VAST_LOG_ACTOR_VERBOSE("attempts to extract timestamp from field " <<
                           timestamp_field_);
  }
  else
  {
    size_t i = 0;
    for (auto& f : flat.fields())
    {
      if (is<time_point>(f.type))
      {
        VAST_LOG_ACTOR_VERBOSE("auto-detected field " << i <<
                               " as event timestamp");
        timestamp_field_ = static_cast<int>(i);
        break;
      }

      ++i;
    }
  }

  return nothing;
}

} // namespace source
} // namespace vast

