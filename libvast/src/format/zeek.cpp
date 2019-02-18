/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include <fstream>
#include <iomanip>

#include <caf/none.hpp>

#include "vast/concept/parseable/vast/port.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/detail/fdoutbuf.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/format/zeek.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast::format::zeek {
namespace {

// The type name prefix to preprend to zeek log names when transleting them
// into VAST types.
constexpr std::string_view type_name_prefix = "zeek.";

// Creates a VAST type from an ASCII Zeek type in a log header.
expected<type> parse_type(std::string_view zeek_type) {
  type t;
  if (zeek_type == "enum" || zeek_type == "string" || zeek_type == "file")
    t = string_type{};
  else if (zeek_type == "bool")
    t = boolean_type{};
  else if (zeek_type == "int")
    t = integer_type{};
  else if (zeek_type == "count")
    t = count_type{};
  else if (zeek_type == "double")
    t = real_type{};
  else if (zeek_type == "time")
    t = timestamp_type{};
  else if (zeek_type == "interval")
    t = timespan_type{};
  else if (zeek_type == "pattern")
    t = pattern_type{};
  else if (zeek_type == "addr")
    t = address_type{};
  else if (zeek_type == "subnet")
    t = subnet_type{};
  else if (zeek_type == "port")
    t = port_type{};
  if (caf::holds_alternative<none_type>(t)
      && (detail::starts_with(zeek_type, "vector")
          || detail::starts_with(zeek_type, "set")
          || detail::starts_with(zeek_type, "table"))) {
    // Zeek's logging framwork cannot log nested vectors/sets/tables, so we can
    // safely assume that we're dealing with a basic type inside the brackets.
    // If this will ever change, we'll have to enhance this simple parser.
    auto open = zeek_type.find("[");
    auto close = zeek_type.rfind("]");
    if (open == std::string::npos || close == std::string::npos)
      return make_error(ec::format_error, "missing container brackets:",
                        std::string{zeek_type});
    auto elem = parse_type(zeek_type.substr(open + 1, close - open - 1));
    if (!elem)
      return elem.error();
    // Zeek sometimes logs sets as tables, e.g., represents set[string] as
    // table[string]. We iron out this inconsistency by normalizing the type to
    // a set.
    if (detail::starts_with(zeek_type, "vector"))
      t = vector_type{*elem};
    else
      t = set_type{*elem};
  }
  if (caf::holds_alternative<none_type>(t))
    return make_error(ec::format_error, "failed to parse type: ",
                      std::string{zeek_type});
  return t;
}

struct zeek_type_printer {
  template <class T>
  std::string operator()(const T& x) const {
    return to_string(x);
  }

  std::string operator()(const real_type&) {
    return "double";
  }

  std::string operator()(const timestamp_type&) {
    return "time";
  }

  std::string operator()(const timespan_type&) {
    return "interval";
  }

  std::string operator()(const vector_type& t) const {
    return "vector[" + caf::visit(*this, t.value_type) + ']';
  }

  std::string operator()(const set_type& t) const {
    return "set[" + caf::visit(*this, t.value_type) + ']';
  }

  std::string operator()(const alias_type& t) const {
    return caf::visit(*this, t.value_type);
  }
};

expected<std::string> to_zeek_string(const type& t) {
  return caf::visit(zeek_type_printer{}, t);
}

constexpr char separator = '\x09';
constexpr char set_separator = ',';
constexpr auto empty_field = "(empty)";
constexpr auto unset_field = "-";

struct time_factory {
  const char* fmt = "%Y-%m-%d-%H-%M-%S";
};

template <class Stream>
Stream& operator<<(Stream& out, const time_factory& t) {
  auto now = std::time(nullptr);
  auto tm = *std::localtime(&now);
  out << std::put_time(&tm, t.fmt);
  return out;
}

void print_header(const type& t, std::ostream& out) {
  VAST_ASSERT(detail::starts_with(t.name(), type_name_prefix));
  auto path = t.name().substr(type_name_prefix.size());
  out << "#separator " << separator << '\n'
      << "#set_separator" << separator << set_separator << '\n'
      << "#empty_field" << separator << empty_field << '\n'
      << "#unset_field" << separator << unset_field << '\n'
      << "#path" << separator << path + '\n'
      << "#open" << separator << time_factory{} << '\n'
      << "#fields";
  auto r = caf::get<record_type>(t);
  for (auto& e : record_type::each{r})
    out << separator << to_string(e.key());
  out << "\n#types";
  for (auto& e : record_type::each{r})
    out << separator << to_zeek_string(e.trace.back()->type);
  out << '\n';
}

struct streamer {
  streamer(std::ostream& out) : out_{out} {
  }

  template <class T>
  void operator()(const T&, caf::none_t) const {
    out_ << unset_field;
  }

  template <class T, class U>
  auto operator()(const T&, const U& x) const
  -> std::enable_if_t<!std::is_same_v<U, caf::none_t>> {
    out_ << to_string(x);
  }

  void operator()(const integer_type&, integer i) const {
    out_ << i;
  }

  void operator()(const count_type&, count c) const {
    out_ << c;
  }

  void operator()(const real_type&, real r) const {
    auto p = real_printer<real, 6, 6>{};
    auto out = std::ostreambuf_iterator<char>(out_);
    p.print(out, r);
  }

  void operator()(const timestamp_type&, timestamp ts) const {
    double d;
    convert(ts.time_since_epoch(), d);
    auto p = real_printer<real, 6, 6>{};
    auto out = std::ostreambuf_iterator<char>(out_);
    p.print(out, d);
  }

  void operator()(const timespan_type&, timespan span) const {
    double d;
    convert(span, d);
    auto p = real_printer<real, 6, 6>{};
    auto out = std::ostreambuf_iterator<char>(out_);
    p.print(out, d);
  }

  void operator()(const string_type&, const std::string& str) const {
    auto out = std::ostreambuf_iterator<char>{out_};
    auto f = str.begin();
    auto l = str.end();
    for ( ; f != l; ++f)
      if (!std::isprint(*f) || *f == separator || *f == set_separator)
        detail::hex_escaper(f, out);
      else
        out_ << *f;
  }

  void operator()(const port_type&, const port& p) const {
    out_ << p.number();
  }

  void operator()(const record_type& r, const vector& v) const {
    VAST_ASSERT(!v.empty());
    VAST_ASSERT(r.fields.size() == v.size());
    caf::visit(*this, r.fields[0].type, v[0]);
    for (auto i = 1u; i < v.size(); ++i) {
      out_ << separator;
      caf::visit(*this, r.fields[i].type, v[i]);
    }
  }

  void operator()(const vector_type& t, const vector& v) const {
    stream(v, t.value_type, set_separator);
  }

  void operator()(const set_type& t, const set& s) const {
    stream(s, t.value_type, set_separator);
  }

  void operator()(const map_type&, const map&) const {
    VAST_ASSERT(!"not supported by Zeek's log format.");
  }

  template <class Container, class Sep>
  void stream(Container& c, const type& value_type, const Sep& sep) const {
    if (c.empty()) {
      // Cannot occur if we have a record
      out_ << empty_field;
      return;
    }
    auto f = c.begin();
    auto l = c.end();
    caf::visit(*this, value_type, *f);
    while (++f != l) {
      out_ << sep;
      caf::visit(*this, value_type, *f);
    }
  }

  std::ostream& out_;
};

} // namespace <anonymous>

reader::reader(caf::atom_value table_slice_type,
               std::unique_ptr<std::istream> in)
  : super(table_slice_type) {
  if (in != nullptr)
    reset(std::move(in));
}

void reader::reset(std::unique_ptr<std::istream> in) {
  VAST_ASSERT(in != nullptr);
  input_ = std::move(in);
  lines_ = std::make_unique<detail::line_range>(*input_);
}

caf::error reader::schema(vast::schema sch) {
  schema_ = std::move(sch);
  return caf::none;
}

schema reader::schema() const {
  vast::schema result;
  result.add(type_);
  return result;
}

const char* reader::name() const {
  return "zeek-reader";
}

void reader::patch(std::vector<data>& xs) {
  auto protocol = port::unknown;
  // Get the protocol from the proto field if available.
  if (proto_field_) {
    VAST_ASSERT(*proto_field_ < xs.size());
    if (auto proto_string = caf::get_if<std::string>(&xs[*proto_field_])) {
      auto p = parsers::port_type >> parsers::eoi;
      if (!p(*proto_string, protocol))
        VAST_DEBUG(this, "could not parse protocol", *proto_string);
    }
  }
  // Or use a simple heuristic if not.
  else {
    if (type_.name() == "zeek.ftp" || type_.name() == "zeek.http"
        || type_.name() == "zeek.irc" || type_.name() == "zeek.rdp"
        || type_.name() == "zeek.smtp" || type_.name() == "zeek.ssh"
        || type_.name() == "zeek.xmpp")
      protocol = port::tcp;
    else if (type_.name() == "zeek.dhcp" || type_.name() == "zeek.dns"
             || type_.name() == "zeek.smnp")
      protocol = port::udp;
  }
  // Assign the deduced proto to all port fields.
  for (auto& pf : port_fields_) {
    auto& p = caf::get<port>(xs[pf]);
    p.type(protocol);
  }
}

caf::error reader::read_impl(size_t max_events, size_t max_slice_size,
                             consumer& f) {
  // Sanity checks.
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(max_slice_size > 0);
  // EOF check.
  if (lines_->done())
    return make_error(ec::end_of_input, "input exhausted");
  // Make sure we have a builder.
  if (builder_ == nullptr) {
    VAST_ASSERT(layout_.fields.empty());
    if (auto err = parse_header())
      return err;
    if (!reset_builder(layout_))
      return make_error(ec::parse_error,
                        "unable to create a bulider for parsed layout at",
                        lines_->line_number());
    // EOF check.
    if (lines_->done())
      return make_error(ec::end_of_input, "input exhausted");
  }
  // Local buffer for parsing records.
  std::vector<data> xs;
  // Counts successfully parsed records.
  size_t produced = 0;
  // Loop until reaching EOF or the configured limit of records.
  while (produced < max_events) {
    // Advance line range and check for EOF.
    lines_->next();
    if (lines_->done())
      return finish(f, make_error(ec::end_of_input, "input exhausted"));
    // Parse curent line.
    auto& line = lines_->get();
    if (line.empty()) {
      // Ignore empty lines.
      VAST_DEBUG(this, "ignores empty line at", lines_->line_number());
    } else if (detail::starts_with(line, "#separator")) {
      // We encountered a new log file.
      if (auto err = finish(f))
        return err;
      VAST_DEBUG(this, "restarts with new log");
      separator_.clear();
      if (auto err = parse_header())
        return err;
      if (!reset_builder(layout_))
        return make_error(ec::parse_error,
                          "unable to create a bulider for parsed layout at",
                          lines_->line_number());
    } else if (detail::starts_with(line, "#")) {
      // Ignore comments.
      VAST_DEBUG(this, "ignores comment at line", lines_->line_number());
    } else {
      auto fields = detail::split(lines_->get(), separator_);
      if (fields.size() != parsers_.size()) {
        VAST_WARNING(this, "ignores invalid record at line",
                     lines_->line_number(), ':', "got", fields.size(),
                     "fields but need", parsers_.size());
        continue;
      }
      // Construct the record.
      auto is_unset = [&](auto i) {
        return std::equal(unset_field_.begin(), unset_field_.end(),
                          fields[i].begin(), fields[i].end());
      };
      auto is_empty = [&](auto i) {
        return std::equal(empty_field_.begin(), empty_field_.end(),
                          fields[i].begin(), fields[i].end());
      };
      xs.resize(fields.size());
      for (size_t i = 0; i < fields.size(); ++i) {
        if (is_unset(i))
          xs[i] = caf::none;
        else if (is_empty(i))
          xs[i] = construct(layout_.fields[i].type);
        else if (!parsers_[i](fields[i], xs[i]))
            return finish(f, make_error(ec::parse_error, "field", i, "line",
                                        lines_->line_number(),
                                        std::string{fields[i]}));
      }
      patch(xs);
      for (size_t i = 0; i < fields.size(); ++i) {
        if (!builder_->add(make_data_view(xs[i])))
          return finish(f, make_error(ec::type_clash, "field", i, "line",
                                      lines_->line_number(),
                                      std::string{fields[i]}));
      }
      if (builder_->rows() == max_slice_size)
        if (auto err = finish(f))
          return err;
      ++produced;
    }
  }
  return finish(f);
}

// Parses a single header line a Zeek log. (Since parsing headers is not on the
// critical path, we are "lazy" and return strings instead of string views.)
expected<std::string> parse_header_line(const std::string& line,
                                        const std::string& sep,
                                        const std::string& prefix) {
  auto s = detail::split(line, sep, "", 1);
  if (!(s.size() == 2
        && std::equal(prefix.begin(), prefix.end(), s[0].begin(), s[0].end())))
    return make_error(ec::format_error, "got invalid header line: " + line);
  return std::string{s[1]};
}

caf::error reader::parse_header() {
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
  const char* prefixes[] = {
    "#set_separator",
    "#empty_field",
    "#unset_field",
    "#path",
    "#open",
    "#fields",
    "#types",
  };
  std::vector<std::string> header(sizeof(prefixes) / sizeof(const char*));
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
  auto fields = detail::split(header[5], separator_);
  auto types = detail::split(header[6], separator_);
  if (fields.size() != types.size())
    return make_error(ec::format_error, "fields and types have different size");
  std::vector<record_field> record_fields;
  proto_field_ = caf::none;
  port_fields_.clear();
  for (auto i = 0u; i < fields.size(); ++i) {
    auto t = parse_type(types[i]);
    if (!t)
      return t.error();
    record_fields.emplace_back(std::string{fields[i]}, *t);
    if (fields[i] == "proto" && types[i] == "enum")
      proto_field_ = i;
    if (caf::holds_alternative<port_type>(*t))
      port_fields_.push_back(i);
  }
  // Construct type.
  layout_ = std::move(record_fields);
  layout_.name(std::string{type_name_prefix} + path);
  VAST_DEBUG(this, "parsed zeek header:");
  VAST_DEBUG(this, "    #separator", separator_);
  VAST_DEBUG(this, "    #set_separator", set_separator_);
  VAST_DEBUG(this, "    #empty_field", empty_field_);
  VAST_DEBUG(this, "    #unset_field", unset_field_);
  VAST_DEBUG(this, "    #path", path);
  VAST_DEBUG(this, "    #fields:");
  for (auto i = 0u; i < layout_.fields.size(); ++i)
    VAST_DEBUG(this, "     ", i << ')',
               layout_.fields[i].name << ':', layout_.fields[i].type);
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
  auto ts_pred = [&](auto& field) {
    if (field.name != "ts")
      return false;
    if (!caf::holds_alternative<timestamp_type>(field.type)) {
      VAST_WARNING(this, "encountered ts fields not of type timestamp");
      return false;
    }
    return true;
  };
  auto i = std::find_if(layout_.fields.begin(), layout_.fields.end(), ts_pred);
  if (i != layout_.fields.end()) {
    VAST_DEBUG(this, "auto-detected field",
               std::distance(layout_.fields.begin(), i), "as event timestamp");
    i->type.attributes({{"time"}});
  }
  // After having modified layout attributes, we no longer make changes to the
  // type and can now safely copy it.
  type_ = layout_;
  // Create Zeek parsers.
  auto make_parser = [](const auto& type, const auto& set_sep) {
    return make_zeek_parser<iterator_type>(type, set_sep);
  };
  parsers_.resize(layout_.fields.size());
  for (size_t i = 0; i < layout_.fields.size(); i++)
    parsers_[i] = make_parser(layout_.fields[i].type, set_separator_);
  return caf::none;
}

writer::writer(path dir) {
  if (dir != "-")
    dir_ = std::move(dir);
}

writer::~writer() {
  if (streams_.empty())
    return;
  std::ostringstream ss;
  ss << "#close" << separator << time_factory{} << '\n';
  auto footer = ss.str();
  for (auto& pair : streams_)
    if (pair.second)
      *pair.second << footer;
}

expected<void> writer::write(const event& e) {
  if (!caf::holds_alternative<record_type>(e.type()))
    return make_error(ec::format_error, "cannot process non-record events");
  std::ostream* os = nullptr;
  if (dir_.empty()) {
    if (streams_.empty()) {
      VAST_DEBUG(this, "creates a new stream for STDOUT");
      auto sb = std::make_unique<detail::fdoutbuf>(1);
      auto out = std::make_unique<std::ostream>(sb.release());
      streams_.emplace("", std::move(out));
    }
    os = streams_.begin()->second.get();
    if (e.type() != previous_layout_) {
      print_header(e.type(), *os);
      previous_layout_ = e.type();
    }
  } else {
    auto i = streams_.find(e.type().name());
    if (i != streams_.end()) {
      os = i->second.get();
      VAST_ASSERT(os != nullptr);
    } else {
      VAST_DEBUG(this, "creates new stream for event", e.type().name());
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
      print_header(e.type(), *fos);
      auto i = streams_.emplace(e.type().name(), std::move(fos));
      os = i.first->second.get();
    }
  }
  VAST_ASSERT(os != nullptr);
  caf::visit(streamer{*os}, e.type(), e.data());
  *os << '\n';
  return no_error;
}

expected<void> writer::flush() {
  for (auto& pair : streams_)
    if (pair.second)
      pair.second->flush();
  return no_error;
}

const char* writer::name() const {
  return "zeek-writer";
}

} // namespace vast::format::zeek
