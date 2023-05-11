//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/zeek.hpp"

#include "vast/cast.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/view.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/detail/fdinbuf.hpp"
#include "vast/detail/fdostream.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/zeekify.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/policy/flatten_schema.hpp"
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
  if (zeek_type == "enum" || zeek_type == "string" || zeek_type == "file"
      || zeek_type == "pattern")
    t = type{string_type{}};
  else if (zeek_type == "bool")
    t = type{bool_type{}};
  else if (zeek_type == "int")
    t = type{int64_type{}};
  else if (zeek_type == "count")
    t = type{uint64_type{}};
  else if (zeek_type == "double")
    t = type{double_type{}};
  else if (zeek_type == "time")
    t = type{time_type{}};
  else if (zeek_type == "interval")
    t = type{duration_type{}};
  else if (zeek_type == "addr")
    t = type{ip_type{}};
  else if (zeek_type == "subnet")
    t = type{subnet_type{}};
  else if (zeek_type == "port")
    // FIXME: once we ship with builtin type aliases, we should reference the
    // port alias type here. Until then, we create the alias manually.
    // See also:
    // - src/format/pcap.cpp
    t = type{"port", uint64_type{}};
  if (!t
      && (zeek_type.starts_with("vector") || zeek_type.starts_with("set")
          || zeek_type.starts_with("table"))) {
    // Zeek's logging framwork cannot log nested vectors/sets/tables, so we can
    // safely assume that we're dealing with a basic type inside the brackets.
    // If this will ever change, we'll have to enhance this simple parser.
    auto open = zeek_type.find('[');
    auto close = zeek_type.rfind(']');
    if (open == std::string::npos || close == std::string::npos)
      return caf::make_error(ec::format_error, "missing container brackets:",
                             std::string{zeek_type});
    auto elem = parse_type(zeek_type.substr(open + 1, close - open - 1));
    if (!elem)
      return elem.error();
    // Zeek sometimes logs sets as tables, e.g., represents set[string] as
    // table[string]. In VAST, they are all lists.
    t = type{list_type{*elem}};
  }
  if (!t)
    return caf::make_error(ec::format_error,
                           "failed to parse type: ", std::string{zeek_type});
  return t;
}

std::string to_zeek_string(const type& t) {
  auto f = detail::overload{
    [](const bool_type&) -> std::string {
      return "bool";
    },
    [](const int64_type&) -> std::string {
      return "int";
    },
    [&](const uint64_type&) -> std::string {
      return t.name() == "port" ? "port" : "count";
    },
    [](const double_type&) -> std::string {
      return "double";
    },
    [](const duration_type&) -> std::string {
      return "interval";
    },
    [](const time_type&) -> std::string {
      return "time";
    },
    [](const string_type&) -> std::string {
      return "string";
    },
    [](const ip_type&) -> std::string {
      return "addr";
    },
    [](const subnet_type&) -> std::string {
      return "subnet";
    },
    [](const enumeration_type&) -> std::string {
      return "enumeration";
    },
    [](const list_type& lt) -> std::string {
      return fmt::format("vector[{}]", to_zeek_string(lt.value_type()));
    },
    [](const map_type&) -> std::string {
      return "map";
    },
    [](const record_type&) -> std::string {
      return "record";
    },
  };
  return t ? caf::visit(f, t) : "none";
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
  if (path.starts_with("zeek."))
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
  for (const auto& [_, offset] : r.leaves())
    out << separator << to_string(r.key(offset));
  out << "\n#types";
  for (const auto& [field, _] : r.leaves())
    out << separator << to_zeek_string(field.type);
  out << '\n';
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

caf::error reader::module(vast::module mod) {
  module_ = std::move(mod);
  return caf::none;
}

module reader::module() const {
  vast::module result;
  result.add(schema_);
  return result;
}

const char* reader::name() const {
  return "zeek-reader";
}

caf::error
reader::read_impl(size_t max_events, size_t max_slice_size, consumer& f) {
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
    auto timed_out = next_line();
    if (timed_out)
      return ec::stalled;
    if (auto err = parse_header())
      return err;
    if (!reset_builder(schema_))
      return caf::make_error(ec::parse_error,
                             "unable to create a bulider for parsed schema at",
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
      return finish(f, caf::make_error(ec::end_of_input, "input exhausted"),
                    output_schema_);
    if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
        && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
      VAST_DEBUG("{} reached batch timeout", detail::pretty_type_name(this));
      return finish(f, ec::timeout, output_schema_);
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
    } else if (line.starts_with("#separator")) {
      // We encountered a new log file.
      if (auto err = finish(f, caf::none, output_schema_))
        return err;
      VAST_DEBUG("{} restarts with new log", detail::pretty_type_name(this));
      separator_.clear();
      if (auto err = parse_header())
        return err;
      if (!reset_builder(schema_))
        return caf::make_error(
          ec::parse_error, "unable to create a bulider for parsed schema at",
          lines_->line_number());
    } else if (line.starts_with('#')) {
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
          xs[i] = caf::get<record_type>(schema_).field(i).type.construct();
        else if (!parsers_[i](fields[i], xs[i]))
          return finish(f,
                        caf::make_error(ec::parse_error, "field", i, "line",
                                        lines_->line_number(),
                                        std::string{fields[i]}),
                        output_schema_);
      }
      for (size_t i = 0; i < fields.size(); ++i) {
        if (!builder_->add(make_data_view(xs[i])))
          return finish(f,
                        caf::make_error(ec::type_clash, "field", i, "line",
                                        lines_->line_number(),
                                        std::string{fields[i]}),
                        output_schema_);
      }
      if (builder_->rows() == max_slice_size)
        if (auto err = finish(f, {}, output_schema_))
          return err;
      ++produced;
      ++batch_events_;
    }
  }
  return finish(f, {}, output_schema_);
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
    "#set_separator", "#empty_field", "#unset_field", "#path",
    "#open",          "#fields",      "#types",
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
  std::vector<struct record_type::field> record_fields;
  proto_field_.reset();
  for (auto i = 0u; i < fields.size(); ++i) {
    auto t = parse_type(types[i]);
    if (!t)
      return t.error();
    record_fields.push_back({
      std::string{fields[i]},
      *t,
    });
    if (fields[i] == "proto" && types[i] == "enum")
      proto_field_ = i;
  }
  // Construct type.
  auto schema = record_type{record_fields};
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
  schema = detail::zeekify(schema);
  auto name = std::string{type_name_prefix} + path;
  // If a congruent type exists in the module, we give the type in the module
  // precedence.
  output_schema_ = {};
  if (auto* t = module_.find(name)) {
    auto is_castable = can_cast(schema, *t);
    if (!is_castable) {
      VAST_WARN("zeek-reader ignores incompatible schema '{}' from schema "
                "files: {}",
                *t, is_castable.error());
    } else {
      output_schema_ = *t;
    }
  }
  for (auto i = 0u; i < schema.num_fields(); ++i)
    VAST_DEBUG("{}       {}) {} : {}", detail::pretty_type_name(this), i,
               schema.field(i).name, schema.field(i).type);
  // After having modified schema attributes, we no longer make changes to the
  // type and can now safely copy it.
  schema_ = type{name, schema};
  // Create Zeek parsers.
  auto make_parser = [](const auto& type, const auto& set_sep) {
    return make_zeek_parser<iterator_type>(type, set_sep);
  };
  parsers_.resize(schema.num_fields());
  for (size_t i = 0; i < schema.num_fields(); i++)
    parsers_[i] = make_parser(schema.field(i).type, set_separator_);
  return caf::none;
}

writer::writer(const caf::settings& options) {
  auto output = get_or(options, "vast.export.write",
                       vast::defaults::export_::write.data());
  if (output != "-")
    dir_ = std::filesystem::path{std::move(output)};
  show_timestamp_tags_
    = !caf::get_or(options, "vast.export.zeek.disable-timestamp-tags", false);
}

namespace {

template <class Iterator>
class zeek_printer : public printer_base<zeek_printer<Iterator>> {
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

  bool operator()(Iterator& out, const view<bool>& x) const {
    return printers::any.print(out, x ? 'T' : 'F');
  }

  bool operator()(Iterator& out, const view<double>& x) const {
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
    auto f = [&](const auto& x) {
      return (*this)(out, x);
    };
    return caf::visit(f, d);
  }

private:
  static constexpr inline auto zeek_real = real_printer<double, 6, 6>{};
};

/// Owns an `std::ostream` and prints to it for a single schema.
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
    return print<policy::flatten_schema>(
      p, slice, {std::string_view{&separator, 1}, "", "", ""});
  }

  [[nodiscard]] const char* name() const override {
    return "zeek-writer";
  }

  bool show_timestamp_tags_;
};

} // namespace

caf::error writer::write(const table_slice& slice) {
  ostream_writer* child = nullptr;
  auto&& schema = slice.schema();
  if (dir_.string().empty()) {
    if (writers_.empty()) {
      VAST_DEBUG("{} creates a new stream for STDOUT",
                 detail::pretty_type_name(this));
      // TODO
      VAST_DIAGNOSTIC_PUSH
      VAST_DIAGNOSTIC_IGNORE_DEPRECATED
      auto out = std::make_unique<detail::fdostream>(1);
      VAST_DIAGNOSTIC_POP
      writers_.emplace(schema.name(), std::make_unique<writer_child>(
                                        std::move(out), show_timestamp_tags_));
    }
    child = writers_.begin()->second.get();
    if (schema != previous_schema_) {
      print_header(schema, child->out(), show_timestamp_tags_);
      previous_schema_ = schema;
    }
  } else {
    auto i = writers_.find(std::string{schema.name()});
    if (i != writers_.end()) {
      child = i->second.get();
    } else {
      VAST_DEBUG("{} creates new stream for schema {}",
                 detail::pretty_type_name(this), schema.name());
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
      auto filename = dir_ / fmt::format("{}.log", schema.name());
      auto fos = std::make_unique<std::ofstream>(filename.string());
      print_header(schema, *fos, show_timestamp_tags_);
      auto i = writers_.emplace(
        schema.name(),
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
  return {};
}

const char* writer::name() const {
  return "zeek-writer";
}

} // namespace vast::format::zeek
