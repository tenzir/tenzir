#include "vast/actor/source/bro.hpp"
#include "vast/concept/parseable/vast/detail/bro_parser_factory.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/util/assert.hpp"
#include "vast/util/string.hpp"

namespace vast {
namespace source {

namespace {

// Creates a VAST type from an ASCII Bro type in a log header.
trial<type> make_type(std::string const& bro_type) {
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
  if (is<none>(t) && (util::starts_with(bro_type, "vector")
                      || util::starts_with(bro_type, "set")
                      || util::starts_with(bro_type, "table"))) {
    // Bro's logging framwork cannot log nested vectors/sets/tables, so we can
    // safely assume that we're dealing with a basic type inside the brackets.
    // If this will ever change, we'll have to enhance this simple parser.
    auto open = bro_type.find("[");
    auto close = bro_type.rfind("]");
    if (open == std::string::npos || close == std::string::npos)
      return error{"missing delimiting container brackets: ", bro_type};
    auto elem = make_type(bro_type.substr(open + 1, close - open - 1));
    if (!elem)
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

// Parses a single header line a Bro log.
trial<std::string> parse_header_line(std::string const& line,
                                     std::string const& sep,
                                     std::string const& prefix) {
  auto s = util::split(line, sep, "", 1);
  if (!(s.size() == 2
        && std::equal(prefix.begin(), prefix.end(), s[0].first, s[0].second)))
    return error{"got invalid header line: " + line};
  return std::string{s[1].first, s[1].second};
}

} // namespace <anonymous>

bro_state::bro_state(local_actor* self)
  : line_based_state{self, "bro-source"} {
}

schema bro_state::schema() {
  if (is<none>(type_)) {
    // If the type is not set, we assume the input has not yet been accessed
    // and attempt to parse the Bro log header.
    if (!next_line()) {
      VAST_ERROR_AT(self, "could not read first line of header");
      return {};
    }
    auto t = parse_header();
    if (!t) {
      VAST_ERROR_AT(self, "failed to parse header:", t.error());
      return {};
    }
  }
  vast::schema sch;
  sch.add(type_);
  return sch;
}

void bro_state::schema(vast::schema const& sch) {
  schema_ = sch;
}

result<event> bro_state::extract() {
  if (is<none>(type_)) {
    if (!next_line())
      return error{"could not read first line of header"};
    auto t = parse_header();
    if (!t)
      return t.error();
  }
  // Check if we've reached EOF.
  if (!next_line())
    return {};
  // Check if we encountered a new log file.
  auto s = util::split(line, separator_);
  if (s.size() > 0 && s[0].first != s[0].second && *s[0].first == '#') {
    if (util::starts_with(s[0].first, s[0].second, "#separator")) {
      VAST_VERBOSE_AT(self, "restarts with new log");
      timestamp_field_ = -1;
      separator_ = " ";
      auto t = parse_header();
      if (!t)
        return t.error();
      if (!next_line())
        return {};
      s = util::split(line, separator_);
    } else {
      VAST_VERBOSE_AT(self, "ignored comment at line", line_no << ':', line);
      return {};
    }
  }
  size_t f = 0;
  size_t depth = 1;
  record event_record;
  record* r = &event_record;
  auto ts = time::now();
  for (auto& e : type::record::each{*get<type::record>(type_)}) {
    if (f == s.size()) {
      VAST_WARN_AT(self, "accessed field", f, "out of bounds");
      return {};
    }
    if (e.trace.size() > depth) {
      for (size_t i = 0; i < e.depth() - depth; ++i) {
        ++depth;
        r->push_back(record{});
        r = get<record>(r->back());
      }
    } else if (e.depth() < depth) {
      r = &event_record;
      depth = e.depth();
      for (size_t i = 0; i < depth - 1; ++i)
        r = get<record>(r->back());
    }
    if (std::equal(unset_field_.begin(), unset_field_.end(), s[f].first,
                   s[f].second)) {
      r->emplace_back(nil);
    } else if (std::equal(empty_field_.begin(), empty_field_.end(), s[f].first,
                          s[f].second)) {
      switch (which(e.trace.back()->type)) {
        default:
          VAST_WARN_AT(self, "got invalid empty field", f,
                       '"' << e.trace.back()->name << "\" of type",
                       e.trace.back()->type << ':',
                       std::string(s[f].first, s[f].second));
          return {};
        case type::tag::string:
          r->emplace_back(std::string{});
          break;
        case type::tag::vector:
          r->emplace_back(vector{});
          break;
        case type::tag::set:
          r->emplace_back(vast::set{});
          break;
        case type::tag::table:
          r->emplace_back(table{});
          break;
      }
    } else {
      data d;
      if (!parsers_[f].parse(s[f].first, s[f].second, d)) {
        VAST_WARN_AT(self, "failed to parse field", f << ':',
                     std::string(s[f].first, s[f].second));
        VAST_WARN_AT(self, "skips line:", line);
        return {};
      }
      // Get the event timestamp if we're at the timestamp field.
      if (f == static_cast<size_t>(timestamp_field_))
        if (auto tp = get<time::point>(d))
          ts = *tp;
      r->push_back(std::move(d));
    }
    ++f;
  }
  event e{{std::move(event_record), type_}};
  e.timestamp(ts);
  return e;
}

trial<void> bro_state::parse_header() {
  auto header_value = parse_header_line(line, separator_, "#separator");
  if (!header_value)
    return header_value.error();
  // Parse #separator_.
  std::string sep;
  std::string::size_type pos = 0;
  while (pos != std::string::npos) {
    pos = header_value->find("\\x", pos);
    if (pos != std::string::npos) {
      auto i = std::stoi(header_value->substr(pos + 2, 2), nullptr, 16);
      VAST_ASSERT(i >= 0 && i <= 255);
      sep.push_back(i);
      pos += 2;
    }
  }
  separator_ = {sep.begin(), sep.end()};
  // Parse #set_separator.
  if (!next_line())
    return error{"failed to retrieve next header line"};
  header_value = parse_header_line(line, separator_, "#set_separator");
  if (!header_value)
    return header_value.error();
  set_separator_ = std::move(*header_value);
  // Parse #empty_field.
  if (!next_line())
    return error{"failed to retrieve next header line"};
  header_value = parse_header_line(line, separator_, "#empty_field");
  if (!header_value)
    return header_value.error();
  empty_field_ = std::move(*header_value);
  // Parse #unset_field.
  if (!next_line())
    return error{"failed to retrieve next header line"};
  header_value = parse_header_line(line, separator_, "#unset_field");
  if (!header_value)
    return header_value.error();
  unset_field_ = std::move(*header_value);
  // Parse #path.
  if (!next_line())
    return error{"failed to retrieve next header line"};
  header_value = parse_header_line(line, separator_, "#path");
  // Parse #fields
  if (!header_value)
    return header_value.error();
  auto event_name = std::move(*header_value);
  if (!(next_line() && next_line())) // Skip #open tag.
    return error{"failed to retrieve next header line"};
  header_value = parse_header_line(line, separator_, "#fields");
  if (!header_value)
    return header_value.error();
  auto field_names = util::to_strings(util::split(*header_value, separator_));
  // Parse #types.
  if (!next_line())
    return error{"failed to retrieve next header line"};
  header_value = parse_header_line(line, separator_, "#types");
  if (!header_value)
    return header_value.error();
  // Create fields.
  auto field_types = util::to_strings(util::split(*header_value, separator_));
  if (field_types.size() != field_names.size())
    return error{"differing size of field names and field types"};
  std::vector<type::record::field> fields;
  for (size_t i = 0; i < field_types.size(); ++i) {
    auto t = make_type(field_types[i]);
    if (!t)
      return t.error();
    fields.emplace_back(field_names[i], *t);
  }
  // Construct type.
  type::record flat{std::move(fields)};
  type_ = unflatten(flat);
  type_.name("bro::" + event_name);
  VAST_VERBOSE_AT(self, "parsed bro header:");
  VAST_VERBOSE_AT(self, "    #separator", separator_);
  VAST_VERBOSE_AT(self, "    #set_separator", set_separator_);
  VAST_VERBOSE_AT(self, "    #empty_field", empty_field_);
  VAST_VERBOSE_AT(self, "    #unset_field", unset_field_);
  VAST_VERBOSE_AT(self, "    #path", event_name);
  VAST_VERBOSE_AT(self, "    #fields:");
  for (size_t i = 0; i < flat.fields().size(); ++i)
    VAST_VERBOSE_AT(self, "     ", i << ')', flat.fields()[i]);
  // If a congruent type exists in the schema, we give the schema type
  // precedence because it may have user-annotated extra information.
  if (auto t = schema_.find(event_name))
    if (t->name() == event_name) {
      if (congruent(type_, *t)) {
        VAST_VERBOSE_AT(self, "prefers type in schema over type in header");
        type_ = *t;
      } else {
        VAST_WARN_AT(self, "ignores incongruent types in schema and log:",
                     t->name());
      }
    }
  // Determine the timestamp field.
  if (timestamp_field_ > -1) {
    VAST_VERBOSE_AT(self, "uses event timestamp from field", timestamp_field_);
  } else {
    size_t i = 0;
    for (auto& f : flat.fields()) {
      if (is<type::time_point>(f.type)) {
        VAST_VERBOSE_AT(self, "auto-detected field", i, "as event timestamp");
        timestamp_field_ = static_cast<int>(i);
        break;
      }
      ++i;
    }
  }
  // Create Bro parsers.
  auto make_parser = [](auto const& type, auto const& set_sep) {
    using iterator_type = std::string::const_iterator;
    return detail::make_bro_parser<iterator_type>(type, set_sep);
  };
  parsers_.resize(flat.fields().size());
  for (size_t i = 0; i < flat.fields().size(); i++)
    parsers_[i] = make_parser(flat.fields()[i].type, set_separator_);
  return nothing;
}

behavior bro(stateful_actor<bro_state>* self,
             std::unique_ptr<std::istream> in) {
  return line_based(self, std::move(in));
}

} // namespace source
} // namespace vast
