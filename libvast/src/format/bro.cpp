#include <fstream>
#include <iomanip>

#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/fdoutbuf.hpp"
#include "vast/detail/string.hpp"
#include "vast/event.hpp"
#include "vast/logger.hpp"

#include "vast/format/bro.hpp"

namespace vast {
namespace format {
namespace bro {
namespace {

// Creates a VAST type from an ASCII Bro type in a log header.
expected<type> parse_type(std::string const& bro_type) {
  type t;
  if (bro_type == "enum" || bro_type == "string" || bro_type == "file")
    t = string_type{};
  else if (bro_type == "bool")
    t = boolean_type{};
  else if (bro_type == "int")
    t = integer_type{};
  else if (bro_type == "count")
    t = count_type{};
  else if (bro_type == "double")
    t = real_type{};
  else if (bro_type == "time")
    t = timestamp_type{};
  else if (bro_type == "interval")
    t = interval_type{};
  else if (bro_type == "pattern")
    t = pattern_type{};
  else if (bro_type == "addr")
    t = address_type{};
  else if (bro_type == "subnet")
    t = subnet_type{};
  else if (bro_type == "port")
    t = port_type{};
  if (is<none_type>(t) && (detail::starts_with(bro_type, "vector")
                           || detail::starts_with(bro_type, "set")
                           || detail::starts_with(bro_type, "table"))) {
    // Bro's logging framwork cannot log nested vectors/sets/tables, so we can
    // safely assume that we're dealing with a basic type inside the brackets.
    // If this will ever change, we'll have to enhance this simple parser.
    auto open = bro_type.find("[");
    auto close = bro_type.rfind("]");
    if (open == std::string::npos || close == std::string::npos)
      return make_error(ec::format_error, "missing container brackets:",
                        bro_type);
    auto elem = parse_type(bro_type.substr(open + 1, close - open - 1));
    if (!elem)
      return elem.error();
    // Bro sometimes logs sets as tables, e.g., represents set[string] as
    // table[string]. We iron out this inconsistency by normalizing the type to
    // a set.
    if (detail::starts_with(bro_type, "vector"))
      t = vector_type{*elem};
    else
      t = set_type{*elem};
  }
  if (is<none_type>(t))
    return make_error(ec::format_error, "failed to parse type: ", bro_type);
  return t;
}

struct bro_type_printer {
  template <class T>
  std::string operator()(T const& x) const {
    return to_string(x);
  }

  std::string operator()(real_type const&) {
    return "double";
  }

  std::string operator()(timestamp_type const&) {
    return "time";
  }

  std::string operator()(interval_type const&) {
    return "interval";
  }

  std::string operator()(vector_type const& t) const {
    return "vector[" + visit(*this, t.value_type) + ']';
  }

  std::string operator()(set_type const& t) const {
    return "set[" + visit(*this, t.value_type) + ']';
  }

  std::string operator()(alias_type const& t) const {
    return visit(*this, t.value_type);
  }
};

expected<std::string> to_bro_string(type const& t) {
  return visit(bro_type_printer{}, t);
}

constexpr char separator = '\x09';
constexpr char set_separator = ',';
constexpr auto empty_field = "(empty)";
constexpr auto unset_field = "-";

struct time_factory {
  char const* fmt = "%Y-%m-%d-%H-%M-%S";
};

template <class Stream>
Stream& operator<<(Stream& out, time_factory const& t) {
  auto now = std::time(nullptr);
  auto tm = *std::localtime(&now);
  out << std::put_time(&tm, t.fmt);
  return out;
}

void stream_header(type const& t, std::ostream& out) {
  auto i = t.name().find("bro::");
  auto path = i == std::string::npos ? t.name() : t.name().substr(5);
  out << "#separator " << separator << '\n'
      << "#set_separator" << separator << set_separator << '\n'
      << "#empty_field" << separator << empty_field << '\n'
      << "#unset_field" << separator << unset_field << '\n'
      << "#path" << separator << path + '\n'
      << "#open" << separator << time_factory{} << '\n'
      << "#fields";
  auto r = get<record_type>(t);
  for (auto& e : record_type::each{r})
    out << separator << to_string(e.key());
  out << "\n#types";
  for (auto& e : record_type::each{r})
    out << separator << to_bro_string(e.trace.back()->type);
  out << '\n';
}

struct streamer {
  streamer(std::ostream& out) : out_{out} {
  }

  template <class T>
  void operator()(T const&, none) const {
    out_ << unset_field;
  }

  template <class T, class U>
  auto operator()(T const&, U const& x) const
  -> std::enable_if_t<!std::is_same<U, none>::value> {
    out_ << to_string(x);
  }

  void operator()(integer_type const&, integer i) const {
    out_ << i;
  }

  void operator()(count_type const&, count c) const {
    out_ << c;
  }

  void operator()(real_type const&, real r) const {
    auto p = real_printer<real, 6>{};
    auto out = std::ostreambuf_iterator<char>(out_);
    p.print(out, r);
  }

  void operator()(timestamp_type const&, timestamp ts) const {
    double d;
    convert(ts.time_since_epoch(), d);
    auto p = real_printer<real, 6>{};
    auto out = std::ostreambuf_iterator<char>(out_);
    p.print(out, d);
  }

  void operator()(interval_type const&, interval i) const {
    double d;
    convert(i, d);
    auto p = real_printer<real, 6>{};
    auto out = std::ostreambuf_iterator<char>(out_);
    p.print(out, d);
  }

  void operator()(string_type const&, std::string const& str) const {
    auto out = std::ostreambuf_iterator<char>{out_};
    auto f = str.begin();
    auto l = str.end();
    for ( ; f != l; ++f)
      if (!std::isprint(*f) || *f == separator || *f == set_separator)
        detail::hex_escaper(f, l, out);
      else
        out_ << *f;
  }

  void operator()(port_type const&, port const& p) const {
    out_ << p.number();
  }

  void operator()(record_type const& r, vector const& v) const {
    VAST_ASSERT(!v.empty());
    VAST_ASSERT(r.fields.size() == v.size());
    visit(*this, r.fields[0].type, v[0]);
    for (auto i = 1u; i < v.size(); ++i) {
      out_ << separator;
      visit(*this, r.fields[i].type, v[i]);
    }
  }

  void operator()(vector_type const& t, vector const& v) const {
    stream(v, t.value_type, set_separator);
  }

  void operator()(set_type const& t, set const& s) const {
    stream(s, t.value_type, set_separator);
  }

  void operator()(table_type const&, table const&) const {
    VAST_ASSERT(!"not supported by Bro's log format.");
  }

  template <class Container, class Sep>
  void stream(Container& c, type const& value_type, Sep const& sep) const {
    if (c.empty()) {
      // Cannot occur if we have a record
      out_ << empty_field;
      return;
    }
    auto f = c.begin();
    auto l = c.end();
    visit(*this, value_type, *f);
    while (++f != l) {
      out_ << sep;
      visit(*this, value_type, *f);
    }
  }

  std::ostream& out_;
};

} // namespace <anonymous>

reader::reader(std::unique_ptr<std::istream> input) : input_{std::move(input)} {
  VAST_ASSERT(input_);
  lines_ = std::make_unique<detail::line_range>(*input_);
}

maybe<event> reader::read() {
  if (lines_->done())
    return make_error(ec::end_of_input, "input exhausted");
  if (is<none_type>(type_)) {
    auto t = parse_header();
    if (!t)
      return t.error();
  }
  // Check if we encountered a new log file.
  lines_->next();
  if (lines_->done())
    return make_error(ec::end_of_input, "input exhausted");
  auto s = detail::split(lines_->get(), separator_);
  if (s.size() > 0 && s[0].first != s[0].second && *s[0].first == '#') {
    if (detail::starts_with(s[0].first, s[0].second, "#separator")) {
      VAST_DEBUG(name(), "restarts with new log");
      timestamp_field_ = -1;
      separator_.clear();
      auto t = parse_header();
      if (!t)
        return t.error();
      lines_->next();
      if (lines_->done())
        return make_error(ec::end_of_input, "input exhausted");
      s = detail::split(lines_->get(), separator_);
    } else {
      VAST_DEBUG(name(), "ignored comment at line",
                 lines_->line_number() << ':', lines_->get());
      return {};
    }
  }
  // Construct the record.
  size_t f = 0;
  size_t depth = 1;
  vector record;
  vector* r = &record;
  optional<timestamp> ts;
  for (auto& e : record_type::each{get<record_type>(type_)}) {
    if (f == s.size()) {
      VAST_WARNING(name(), "accessed field", f, "out of bounds");
      return {};
    }
    if (e.trace.size() > depth) {
      for (size_t i = 0; i < e.depth() - depth; ++i) {
        ++depth;
        r->push_back(vector{});
        r = get_if<vector>(r->back());
      }
    } else if (e.depth() < depth) {
      r = &record;
      depth = e.depth();
      for (size_t i = 0; i < depth - 1; ++i)
        r = get_if<vector>(r->back());
    }
    if (std::equal(unset_field_.begin(), unset_field_.end(), s[f].first,
                   s[f].second)) {
      r->emplace_back(nil);
    } else if (std::equal(empty_field_.begin(), empty_field_.end(), s[f].first,
                          s[f].second)) {
      r->emplace_back(construct(e.trace.back()->type));
    } else {
      data d;
      if (!parsers_[f].parse(s[f].first, s[f].second, d))
        return make_error(ec::parse_error,
                          "field", f, "line", lines_->line_number(),
                          std::string(s[f].first, s[f].second));
      // Get the event timestamp if we're at the timestamp field.
      if (f == static_cast<size_t>(timestamp_field_))
        if (auto tp = get_if<timestamp>(d))
          ts = *tp;
      r->push_back(std::move(d));
    }
    ++f;
  }
  event e{{std::move(record), type_}};
  e.timestamp(ts ? *ts : timestamp::clock::now());
  return e;
}

expected<void> reader::schema(vast::schema const& sch) {
  schema_ = sch;
  return {};
}

expected<schema> reader::schema() const {
  if (is<none_type>(type_))
    return make_error(ec::format_error, "schema not yet inferred");
  vast::schema sch;
  sch.add(type_);
  return sch;
}

const char* reader::name() const {
  return "bro-reader";
}

// Parses a single header line a Bro log. (Since parsing headers is not on the
// critical path, we are "lazy" and return strings instead of string views.)
expected<std::string> parse_header_line(std::string const& line,
                                        std::string const& sep,
                                        std::string const& prefix) {
  auto s = detail::split(line, sep, "", 1);
  if (!(s.size() == 2
        && std::equal(prefix.begin(), prefix.end(), s[0].first, s[0].second)))
    return make_error(ec::format_error, "got invalid header line: " + line);
  return std::string{s[1].first, s[1].second};
}

expected<void> reader::parse_header() {
  // Parse #separator.
  if (lines_->done())
    return make_error(ec::format_error, "not enough header lines");
  auto pos = lines_->get().find("#separator ");
  if (pos != 0)
    return make_error(ec::format_error, "invalid #separator line");
  pos += 11;
  separator_.clear();
  while (pos != std::string::npos) {
    pos = lines_->get().find("\\x", pos);
    if (pos != std::string::npos) {
      auto c = std::stoi(lines_->get().substr(pos + 2, 2), nullptr, 16);
      VAST_ASSERT(c >= 0 && c <= 255);
      separator_.push_back(c);
      pos += 2;
    }
  }
  // Retrieve remaining header lines.
  char const* prefixes[] = {
    "#set_separator",
    "#empty_field",
    "#unset_field",
    "#path",
    "#open",
    "#fields",
    "#types",
  };
  std::vector<std::string> header(sizeof(prefixes) / sizeof(char const*));
  for (auto i = 0u; i < header.size(); ++i) {
    lines_->next();
    if (lines_->done())
      return make_error(ec::format_error, "not enough header lines");
    auto& line = lines_->get();
    pos = line.find(prefixes[i]);
    if (pos != 0)
      return make_error(ec::format_error, "invalid header line, expected",
                        prefixes[i]);
    pos = line.find(separator_);
    if (pos == std::string::npos)
      return make_error(ec::format_error, "invalid separator in header line");
    if (pos + separator_.size() >= line.size())
      return make_error(ec::format_error, "missing header content:", line);
    header[i] = line.substr(pos + separator_.size());
  }
  // Assign header values.
  set_separator_ = std::move(header[0]);
  empty_field_ = std::move(header[1]);
  unset_field_ = std::move(header[2]);
  auto& path = header[3];
  auto fields = detail::to_strings(detail::split(header[5], separator_));
  auto types = detail::to_strings(detail::split(header[6], separator_));
  if (fields.size() != types.size())
    return make_error(ec::format_error, "fields and types have different size");
  std::vector<record_field> record_fields;
  for (auto i = 0u; i < fields.size(); ++i) {
    auto t = parse_type(types[i]);
    if (!t)
      return t.error();
    record_fields.emplace_back(fields[i], *t);
  }
  // Construct type.
  record_type flat{std::move(record_fields)};
  type_ = unflatten(flat);
  type_.name() = "bro::" + path;
  VAST_DEBUG(name(), "parsed bro header:");
  VAST_DEBUG(name(), "    #separator", separator_);
  VAST_DEBUG(name(), "    #set_separator", set_separator_);
  VAST_DEBUG(name(), "    #empty_field", empty_field_);
  VAST_DEBUG(name(), "    #unset_field", unset_field_);
  VAST_DEBUG(name(), "    #path", path);
  VAST_DEBUG(name(), "    #fields:");
  for (auto i = 0u; i < flat.fields.size(); ++i)
    VAST_DEBUG(name(), "     ", i << ')',
               flat.fields[i].name << ':', flat.fields[i].type);
  // If a congruent type exists in the schema, we give the schema type
  // precedence.
  if (auto t = schema_.find(path))
    if (t->name() == path) {
      if (congruent(type_, *t))
        type_ = *t;
      else
        return make_error(ec::format_error, "incongruent types in schema");
    }
  // Determine the timestamp field.
  if (timestamp_field_ > -1) {
    VAST_DEBUG(name(), "uses event timestamp from field", timestamp_field_);
  } else {
    size_t i = 0;
    for (auto& f : flat.fields) {
      if (is<timestamp_type>(f.type)) {
        VAST_INFO(name(), "auto-detected field", i, "as event timestamp");
        timestamp_field_ = static_cast<int>(i);
        break;
      }
      ++i;
    }
  }
  // Create Bro parsers.
  auto make_parser = [](auto const& type, auto const& set_sep) {
    using iterator_type = std::string::const_iterator;
    return make_bro_parser<iterator_type>(type, set_sep);
  };
  parsers_.resize(flat.fields.size());
  for (size_t i = 0; i < flat.fields.size(); i++)
    parsers_[i] = make_parser(flat.fields[i].type, set_separator_);
  return {};
}

writer::writer(path dir) {
  if (dir != "-")
    dir_ = std::move(dir);
}

writer::~writer() {
  std::ostringstream ss;
  ss << "#close" << separator << time_factory{} << '\n';
  auto footer = ss.str();
  for (auto& pair : streams_)
    if (pair.second)
      *pair.second << footer;
}

expected<void> writer::write(event const& e) {
  if (!is<record_type>(e.type()))
    return make_error(ec::format_error, "cannot process non-record events");
  std::ostream* os = nullptr;
  if (dir_.empty()) {
    if (streams_.empty()) {
      VAST_DEBUG(name(), "creates a new stream for STDOUT");
      auto sb = std::make_unique<detail::fdoutbuf>(1);
      auto out = std::make_unique<std::ostream>(sb.release());
      auto i = streams_.emplace("", std::move(out));
      stream_header(e.type(), *i.first->second);
    }
    os = streams_.begin()->second.get();
  } else {
    auto i = streams_.find(e.type().name());
    if (i != streams_.end()) {
      os = i->second.get();
      VAST_ASSERT(os != nullptr);
    } else {
      VAST_DEBUG(name(), "creates new stream for event", e.type().name());
      if (!exists(dir_)) {
        auto d = mkdir(dir_);
        if (!d)
          return d.error();
      } else if (!dir_.is_directory()) {
        return make_error(ec::format_error, "got existing non-directory path",
                          dir_);
      }
      auto filename = dir_ / (e.type().name() + ".log");
      auto fos = std::make_unique<std::ofstream>(filename.str());
      stream_header(e.type(), *fos);
      auto i = streams_.emplace("", std::move(fos));
      os = i.first->second.get();
    }
  }
  VAST_ASSERT(os != nullptr);
  visit(streamer{*os}, e.type(), e.data());
  *os << '\n';
  return {};
}

expected<void> writer::flush() {
  for (auto& pair : streams_)
    if (pair.second)
      pair.second->flush();
  return {};
}

const char* writer::name() const {
  return "bro-writer";
}

} // namespace bro
} // namespace format
} // namespace vast
