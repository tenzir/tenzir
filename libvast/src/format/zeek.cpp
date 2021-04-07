//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/zeek.hpp"

#include "vast/attribute.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/concept/printable/vast/view.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/detail/fdinbuf.hpp"
#include "vast/detail/fdostream.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/policy/flatten_layout.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

#include <caf/none.hpp>
#include <caf/settings.hpp>

#include <fstream>
#include <iomanip>

namespace vast::format::zeek {

namespace {

// The type name prefix to preprend to zeek log names when transleting them
// into VAST types.
constexpr std::string_view type_name_prefix = "zeek.";

// Creates a VAST type from an ASCII Zeek type in a log header.
caf::expected<type> parse_type(std::string_view zeek_type) {
  type t;
  if (zeek_type == "enum" || zeek_type == "string" || zeek_type == "file")
    t = string_type{};
  else if (zeek_type == "bool")
    t = bool_type{};
  else if (zeek_type == "int")
    t = integer_type{};
  else if (zeek_type == "count")
    t = count_type{};
  else if (zeek_type == "double")
    t = real_type{};
  else if (zeek_type == "time")
    t = time_type{};
  else if (zeek_type == "interval")
    t = duration_type{};
  else if (zeek_type == "pattern")
    t = pattern_type{};
  else if (zeek_type == "addr")
    t = address_type{};
  else if (zeek_type == "subnet")
    t = subnet_type{};
  else if (zeek_type == "port")
    // FIXME: once we ship with builtin type aliases, we should reference the
    // port alias type here. Until then, we create the alias manually.
    // See also:
    // - src/format/pcap.cpp
    t = count_type{}.name("port");
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
      return caf::make_error(ec::format_error, "missing container brackets:",
                             std::string{zeek_type});
    auto elem = parse_type(zeek_type.substr(open + 1, close - open - 1));
    if (!elem)
      return elem.error();
    // Zeek sometimes logs sets as tables, e.g., represents set[string] as
    // table[string]. In VAST, they are all lists.
    t = list_type{*elem};
  }
  if (caf::holds_alternative<none_type>(t))
    return caf::make_error(ec::format_error,
                           "failed to parse type: ", std::string{zeek_type});
  return t;
}

struct zeek_type_printer {
  template <class T>
  std::string operator()(const T& x) const {
    return kind(x);
  }

  std::string operator()(const count_type& t) const {
    return t.name() == "port" ? "port" : "count";
  }

  std::string operator()(const real_type&) const {
    return "double";
  }

  std::string operator()(const time_type&) const {
    return "time";
  }

  std::string operator()(const duration_type&) const {
    return "interval";
  }

  std::string operator()(const address_type&) const {
    return "addr";
  }

  std::string operator()(const list_type& t) const {
    return "vector[" + caf::visit(*this, t.value_type) + ']';
  }

  std::string operator()(const alias_type& t) const {
    return caf::visit(*this, t.value_type);
  }
};

auto to_zeek_string(const type& t) {
  return caf::visit(zeek_type_printer{}, t);
}

constexpr char separator = '\x09';
constexpr char set_separator = ',';
constexpr auto empty_field = "(empty)";
constexpr auto unset_field = '-';

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

void print_header(const type& t, std::ostream& out, bool show_timestamp_tags) {
  auto path = std::string_view{t.name()};
  // To ensure that the printed output conforms to standard Zeek naming
  // practices, we strip VAST's internal "zeek." type prefix such that the
  // output is, e.g., "#path conn" instead of "#path zeek.conn". For all
  // non-Zeek types, we keep the prefix to avoid conflicts in tools that work
  // with Zeek TSV.
  if (detail::starts_with(path, "zeek."))
    path = path.substr(5);
  out << "#separator " << separator << '\n'
      << "#set_separator" << separator << set_separator << '\n'
      << "#empty_field" << separator << empty_field << '\n'
      << "#unset_field" << separator << unset_field << '\n'
      << "#path" << separator << path << '\n';
  if (show_timestamp_tags)
    out << "#open" << separator << time_factory{} << '\n';
  out << "#fields";
  auto r = caf::get<record_type>(t);
  for (auto& e : record_type::each{r})
    out << separator << to_string(e.key());
  out << "\n#types";
  for (auto& e : record_type::each{r})
    out << separator << to_zeek_string(e.trace.back()->type);
  out << '\n';
}

// TODO: Find a better place for this function, ideally we want to modify type
// attributes in place.
bool insert_attribute(type& t, attribute a, bool overwrite = false) {
  std::vector<attribute> attrs = t.attributes();
  auto guard
    = caf::detail::make_scope_guard([&]() { t.attributes(std::move(attrs)); });
  auto i = std::find_if(attrs.begin(), attrs.end(),
                        [&](const auto& x) { return x.key == a.key; });
  if (i == attrs.end()) {
    attrs.push_back(std::move(a));
    return true;
  }
  if (!overwrite)
    return false;
  i->value = std::move(a.value);
  return true;
}

void add_hash_index_attribute(record_type& layout) {
  // TODO: do more than this simple heuristic. For example, also consider
  // zeek.files.conn_uids, which is a set of strings. The inner index needs to
  // have the #index=hash tag. There are a lot more cases that we need to
  // consider, such as zeek.x509.id (instead of uid).
  auto pred = [&](auto& field) {
    return caf::holds_alternative<string_type>(field.type)
           && (field.name == "uid" || field.name == "fuid"
               || field.name == "community_id");
  };
  auto& fields = layout.fields;
  auto find = [&](auto i) { return std::find_if(i, fields.end(), pred); };
  for (auto i = find(fields.begin()); i != fields.end(); i = find(i + 1)) {
    VAST_DEBUG("using hash index for field {}", i->name);
    insert_attribute(i->type, {"index", "hash"}, false);
  }
}

} // namespace

reader::reader(const caf::settings& options, std::unique_ptr<std::istream> in)
  : super(options) {
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

caf::error reader::read_impl(size_t max_events, size_t max_slice_size,
                             consumer& f) {
  // Sanity checks.
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(max_slice_size > 0);
  auto next_line = [&] {
    auto timed_out = lines_->next_timeout(read_timeout_);
    if (timed_out)
      VAST_DEBUG("{} reached input timeout at line {}",
                 detail::pretty_type_name(this), lines_->line_number());
    return timed_out;
  };
  // EOF check.
  if (lines_->done())
    return caf::make_error(ec::end_of_input, "input exhausted");
  // Make sure we have a builder.
  if (builder_ == nullptr) {
    VAST_ASSERT(layout_.fields.empty());
    auto timed_out = next_line();
    if (timed_out)
      return ec::stalled;
    if (auto err = parse_header())
      return err;
    if (!reset_builder(layout_))
      return caf::make_error(ec::parse_error,
                             "unable to create a bulider for parsed layout at",
                             lines_->line_number());
    // EOF check.
    if (lines_->done())
      return caf::make_error(ec::end_of_input, "input exhausted");
  }
  // Local buffer for parsing records.
  std::vector<data> xs;
  // Counts successfully parsed records.
  size_t produced = 0;
  // Loop until reaching EOF, a timeout, or the configured limit of records.
  while (produced < max_events) {
    if (lines_->done())
      return finish(f, caf::make_error(ec::end_of_input, "input exhausted"));
    if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
        && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
      VAST_DEBUG("{} reached batch timeout", detail::pretty_type_name(this));
      return finish(f, ec::timeout);
    }
    auto timed_out = next_line();
    if (timed_out)
      return ec::stalled;
    // Parse curent line.
    auto& line = lines_->get();
    if (line.empty()) {
      // Ignore empty lines.
      VAST_DEBUG("{} ignores empty line at {}", detail::pretty_type_name(this),
                 lines_->line_number());
      continue;
    } else if (detail::starts_with(line, "#separator")) {
      // We encountered a new log file.
      if (auto err = finish(f))
        return err;
      VAST_DEBUG("{} restarts with new log", detail::pretty_type_name(this));
      separator_.clear();
      if (auto err = parse_header())
        return err;
      if (!reset_builder(layout_))
        return caf::make_error(
          ec::parse_error, "unable to create a bulider for parsed layout at",
          lines_->line_number());
    } else if (detail::starts_with(line, "#")) {
      // Ignore comments.
      VAST_DEBUG("{} ignores comment at line {}",
                 detail::pretty_type_name(this), lines_->line_number());
    } else {
      auto fields = detail::split(lines_->get(), separator_);
      if (fields.size() != parsers_.size()) {
        VAST_WARN("{} ignores invalid record at line {}: got {}"
                  "fields but need {}",
                  detail::pretty_type_name(this), lines_->line_number(),
                  fields.size(), parsers_.size());
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
          return finish(f, caf::make_error(ec::parse_error, "field", i, "line",
                                           lines_->line_number(),
                                           std::string{fields[i]}));
      }
      for (size_t i = 0; i < fields.size(); ++i) {
        if (!builder_->add(make_data_view(xs[i])))
          return finish(f, caf::make_error(ec::type_clash, "field", i, "line",
                                           lines_->line_number(),
                                           std::string{fields[i]}));
      }
      if (builder_->rows() == max_slice_size)
        if (auto err = finish(f))
          return err;
      ++produced;
      ++batch_events_;
    }
  }
  return finish(f);
}

// Parses a single header line a Zeek log. (Since parsing headers is not on the
// critical path, we are "lazy" and return strings instead of string views.)
caf::expected<std::string>
parse_header_line(const std::string& line, const std::string& sep,
                  const std::string& prefix) {
  auto s = detail::split(line, sep, "", 1);
  if (!(s.size() == 2
        && std::equal(prefix.begin(), prefix.end(), s[0].begin(), s[0].end())))
    return caf::make_error(ec::format_error,
                           "got invalid header line: " + line);
  return std::string{s[1]};
}

caf::error reader::parse_header() {
  // Parse #separator.
  if (lines_->done())
    return caf::make_error(ec::format_error, "not enough header lines");
  auto pos = lines_->get().find("#separator ");
  if (pos != 0)
    return caf::make_error(ec::format_error, "invalid #separator line");
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
      return caf::make_error(ec::format_error, "not enough header lines");
    auto& line = lines_->get();
    pos = line.find(prefixes[i]);
    if (pos != 0)
      return caf::make_error(ec::format_error, "invalid header line, expected",
                             prefixes[i]);
    pos = line.find(separator_);
    if (pos == std::string::npos)
      return caf::make_error(ec::format_error,
                             "invalid separator in header line", line);
    if (pos + separator_.size() >= line.size())
      return caf::make_error(ec::format_error, "missing header content:", line);
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
    return caf::make_error(ec::format_error, "fields and types have different "
                                             "size");
  // TODO: Consider unflattening the incoming layout.
  std::vector<record_field> record_fields;
  proto_field_ = caf::none;
  for (auto i = 0u; i < fields.size(); ++i) {
    auto t = parse_type(types[i]);
    if (!t)
      return t.error();
    record_fields.emplace_back(std::string{fields[i]}, *t);
    if (fields[i] == "proto" && types[i] == "enum")
      proto_field_ = i;
  }
  // Construct type.
  layout_ = record_type{std::move(record_fields)};
  layout_.name(std::string{type_name_prefix} + path);
  VAST_DEBUG("{} parsed zeek header:", detail::pretty_type_name(this));
  VAST_DEBUG("{}     #separator {}", detail::pretty_type_name(this),
             separator_);
  VAST_DEBUG("{}     #set_separator {}", detail::pretty_type_name(this),
             set_separator_);
  VAST_DEBUG("{}     #empty_field {}", detail::pretty_type_name(this),
             empty_field_);
  VAST_DEBUG("{}     #unset_field {}", detail::pretty_type_name(this),
             unset_field_);
  VAST_DEBUG("{}     #path {}", detail::pretty_type_name(this), path);
  VAST_DEBUG("{}     #fields:", detail::pretty_type_name(this));
  // If a congruent type exists in the schema, we give the schema type
  // precedence.
  if (auto t = schema_.find(layout_.name())) {
    auto r = caf::get_if<record_type>(t);
    if (!r)
      return caf::make_error(ec::format_error,
                             "the zeek reader expects records for "
                             "the top level types in the schema");
    auto flat = flatten(*r);
    for (auto& f : flat.fields) {
      auto i = std::find_if(layout_.fields.begin(), layout_.fields.end(),
                            [&](auto& hf) { return hf.name == f.name; });
      if (i != layout_.fields.end()) {
        if (!congruent(i->type, f.type))
          VAST_WARN("{} encountered a type mismatch between the schema "
                    "definition ({}) and the input data ({})",
                    detail::pretty_type_name(this), f, *i);
        else if (!f.type.attributes().empty()) {
          i->type.attributes(f.type.attributes());
        }
      }
    }
  } // We still do attribute inference for the user provided layouts.
  // Determine the timestamp field.
  auto ts_pred = [&](auto& field) {
    if (field.name != "ts")
      return false;
    if (!caf::holds_alternative<time_type>(field.type)) {
      VAST_WARN("{} encountered ts fields not of type timestamp",
                detail::pretty_type_name(this));
      return false;
    }
    return true;
  };
  auto i = std::find_if(layout_.fields.begin(), layout_.fields.end(), ts_pred);
  if (i != layout_.fields.end()) {
    VAST_DEBUG("{} auto-detected field {} as event timestamp",
               detail::pretty_type_name(this),
               std::distance(layout_.fields.begin(), i));
    i->type.name("timestamp");
  }
  // Add #index=hash attribute for fields where it makes sense.
  add_hash_index_attribute(layout_);
  for (auto i = 0u; i < layout_.fields.size(); ++i)
    VAST_DEBUG("{}       {} ) {} : {}", detail::pretty_type_name(this), i,
               layout_.fields[i].name, layout_.fields[i].type);
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

writer::writer(const caf::settings& options) {
  auto output
    = get_or(options, "vast.export.write", vast::defaults::export_::write);
  if (output != "-")
    dir_ = std::filesystem::path{std::move(output)};
  show_timestamp_tags_
    = !caf::get_or(options, "vast.export.zeek.disable-timestamp-tags", false);
}

namespace {

template <class Iterator>
class zeek_printer : public printer<zeek_printer<Iterator>> {
public:
  using attribute = view<data>;

  bool operator()(Iterator& out, view<caf::none_t>) const {
    *out++ = unset_field;
    return true;
  }

  template <class T>
  bool operator()(Iterator& out, const T& x) const {
    if constexpr (std::is_same_v<T, view<list>>) {
      if (x.empty()) {
        for (auto c : std::string_view(empty_field))
          *out++ = c;
        return true;
      }
      auto i = x.begin();
      if (!print(out, *i))
        return false;
      for (++i; i != x.end(); ++i) {
        *out++ = set_separator;
        if (!print(out, *i))
          return false;
      }
      return true;
    } else if constexpr (std::is_same_v<T, view<map>>) {
      VAST_ERROR("{} cannot print maps in Zeek TSV format",
                 detail::pretty_type_name(this));
      return false;
    } else {
      make_printer<T> p;
      return p.print(out, x);
    }
  }

  bool operator()(Iterator& out, const view<real>& x) const {
    return zeek_real.print(out, x);
  }

  bool operator()(Iterator& out, const view<time>& x) const {
    double d;
    convert(x.time_since_epoch(), d);
    return zeek_real.print(out, d);
  }

  bool operator()(Iterator& out, const view<duration>& x) const {
    double d;
    convert(x, d);
    return zeek_real.print(out, d);
  }

  bool operator()(Iterator& out, const view<std::string>& str) const {
    for (auto c : str)
      if (std::iscntrl(c) || c == separator || c == set_separator) {
        auto hex = detail::byte_to_hex(c);
        *out++ = '\\';
        *out++ = 'x';
        *out++ = hex.first;
        *out++ = hex.second;
      } else {
        *out++ = c;
      }
    return true;
  }

  bool print(Iterator& out, const attribute& d) const {
    auto f = [&](const auto& x) { return (*this)(out, x); };
    return caf::visit(f, d);
  }

private:
  static constexpr inline auto zeek_real = real_printer<real, 6, 6>{};
};

/// Owns an `std::ostream` and prints to it for a single layout.
class writer_child : public ostream_writer {
public:
  using super = ostream_writer;

  writer_child(std::unique_ptr<std::ostream> out, bool show_timestamp_tags)
    : super{std::move(out)}, show_timestamp_tags_{show_timestamp_tags} {
  }

  ~writer_child() override {
    if (out_ != nullptr && show_timestamp_tags_)
      *out_ << "#close" << separator << time_factory{} << '\n';
  }

  caf::error write(const table_slice& slice) override {
    zeek_printer<std::back_insert_iterator<std::vector<char>>> p;
    return print<policy::flatten_layout>(
      p, slice, {std::string_view{&separator, 1}, "", "", ""});
  }

  const char* name() const override {
    return "zeek-writer";
  }

  bool show_timestamp_tags_;
};

} // namespace

caf::error writer::write(const table_slice& slice) {
  ostream_writer* child = nullptr;
  auto&& layout = slice.layout();
  if (dir_.string().empty()) {
    if (writers_.empty()) {
      VAST_DEBUG("{} creates a new stream for STDOUT",
                 detail::pretty_type_name(this));
      auto out = std::make_unique<detail::fdostream>(1);
      writers_.emplace(layout.name(), std::make_unique<writer_child>(
                                        std::move(out), show_timestamp_tags_));
    }
    child = writers_.begin()->second.get();
    if (layout != previous_layout_) {
      print_header(layout, child->out(), show_timestamp_tags_);
      previous_layout_ = std::move(layout);
    }
  } else {
    auto i = writers_.find(layout.name());
    if (i != writers_.end()) {
      child = i->second.get();
    } else {
      VAST_DEBUG("{} creates new stream for layout {}",
                 detail::pretty_type_name(this), layout.name());
      std::error_code err{};
      const auto exists = std::filesystem::exists(dir_, err);
      if (err)
        return caf::make_error(ec::filesystem_error,
                               fmt::format("failed to check if file {} exists: "
                                           "{}",
                                           dir_.string(), err.message()));
      if (!exists) {
        std::filesystem::create_directory(dir_, err);
        if (err)
          return caf::make_error(ec::filesystem_error,
                                 fmt::format("failed to create directory {}: "
                                             "{}",
                                             dir_.string(), err.message()));
      } else if (!std::filesystem::is_directory(dir_, err)) {
        return caf::make_error(
          ec::format_error, "got existing non-directory path", dir_.string());
      }
      auto filename = dir_ / (layout.name() + ".log");
      auto fos = std::make_unique<std::ofstream>(filename.string());
      print_header(layout, *fos, show_timestamp_tags_);
      auto i = writers_.emplace(
        layout.name(),
        std::make_unique<writer_child>(std::move(fos), show_timestamp_tags_));
      child = i.first->second.get();
    }
  }
  VAST_ASSERT(child != nullptr);
  return child->write(slice);
}

caf::expected<void> writer::flush() {
  for (auto& kvp : writers_)
    if (auto res = kvp.second->flush(); !res)
      return res.error();
  return caf::no_error;
}

const char* writer::name() const {
  return "zeek-writer";
}

} // namespace vast::format::zeek
