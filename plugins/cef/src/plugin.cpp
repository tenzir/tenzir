//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cef/add.hpp"
#include "cef/parse.hpp"

#include <vast/concept/convertible/to.hpp>
#include <vast/data.hpp>
#include <vast/detail/assert.hpp>
#include <vast/detail/line_range.hpp>
#include <vast/detail/make_io_stream.hpp>
#include <vast/error.hpp>
#include <vast/format/multi_layout_reader.hpp>
#include <vast/format/reader.hpp>
#include <vast/module.hpp>
#include <vast/plugin.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <istream>
#include <memory>

namespace vast::plugins::cef {

class reader : public format::multi_layout_reader {
public:
  using super = multi_layout_reader;

  /// Constructs a CEF reader.
  /// @param options Additional options.
  /// @param in The stream of JSON objects.
  explicit reader(const caf::settings& options, std::unique_ptr<std::istream> in
                                                = nullptr)
    : super(options) {
    if (in != nullptr)
      reset(std::move(in));
  }

  reader(const reader&) = delete;
  reader& operator=(const reader&) = delete;
  reader(reader&&) noexcept = default;
  reader& operator=(reader&&) noexcept = default;
  ~reader() override = default;

  void reset(std::unique_ptr<std::istream> in) override {
    VAST_ASSERT(in != nullptr);
    input_ = std::move(in);
    lines_ = std::make_unique<detail::line_range>(*input_);
  }

  caf::error module(class module) override {
    // Not implemented.
    return caf::none;
  }

  [[nodiscard]] class module module() const override {
    class module result {};
    return result;
  }

  [[nodiscard]] const char* name() const override {
    return "cef-reader";
  }

protected:
  system::report status() const override {
    using namespace std::string_literals;
    uint64_t invalid_line = num_invalid_lines_;
    if (num_invalid_lines_ > 0)
      VAST_WARN("{} failed to parse {} of {} recent lines",
                detail::pretty_type_name(this), num_invalid_lines_, num_lines_);
    num_invalid_lines_ = 0;
    num_lines_ = 0;
    return {.data = {
              {name() + ".invalid-line"s, invalid_line},
            }};
  }

  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& cons) override {
    VAST_ASSERT(max_events > 0);
    VAST_ASSERT(max_slice_size > 0);
    size_t produced = 0;
    table_slice_builder_ptr bptr = nullptr;
    while (produced < max_events) {
      if (lines_->done())
        return finish(cons, caf::make_error(ec::end_of_input, "input "
                                                              "exhausted"));
      if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
          && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
        VAST_DEBUG("{} reached batch timeout", detail::pretty_type_name(this));
        return finish(cons, ec::timeout);
      }
      bool timed_out = lines_->next_timeout(read_timeout_);
      if (timed_out) {
        VAST_DEBUG("{} stalled at line {}", detail::pretty_type_name(this),
                   lines_->line_number());
        return ec::stalled;
      }
      auto& line = lines_->get();
      ++num_lines_;
      if (line.empty()) {
        // Ignore empty lines.
        VAST_DEBUG("{} ignores empty line at {}",
                   detail::pretty_type_name(this), lines_->line_number());
        continue;
      }
      auto msg = to<message_view>(std::string_view{line});
      if (!msg) {
        VAST_WARN("{} failed to parse CEF messge: {}",
                  detail::pretty_type_name(this), msg.error());
        ++num_invalid_lines_;
        continue;
      }
      auto schema = infer(*msg);
      bptr = builder(schema);
      if (bptr == nullptr)
        return caf::make_error(ec::parse_error, "unable to get a builder");
      if (auto err = add(*msg, *bptr)) {
        VAST_WARN("{} failed to parse line {}: {} ({})",
                  detail::pretty_type_name(this), lines_->line_number(), line,
                  err);
        ++num_invalid_lines_;
        continue;
      }
      produced++;
      batch_events_++;
      if (bptr->rows() == max_slice_size)
        if (auto err = finish(cons, bptr))
          return err;
    }
    return finish(cons);
  }

private:
  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  mutable size_t num_invalid_lines_ = 0;
  mutable size_t num_lines_ = 0;
};

class plugin final : public virtual reader_plugin {
  caf::error initialize(data) override {
    return caf::none;
  }

  [[nodiscard]] const char* name() const override {
    return "cef";
  }

  [[nodiscard]] const char* reader_format() const override {
    return "cef";
  }

  [[nodiscard]] const char* reader_help() const override {
    return "imports logs in Common Event Format (CEF)";
  }

  [[nodiscard]] caf::config_option_set
  reader_options(command::opts_builder&&) const override {
    return {};
  }

  [[nodiscard]] std::unique_ptr<format::reader>
  make_reader(const caf::settings& options) const override {
    auto in = detail::make_input_stream(options);
    return std::make_unique<reader>(options, in ? std::move(*in) : nullptr);
  }
};

} // namespace vast::plugins::cef

VAST_REGISTER_PLUGIN(vast::plugins::cef::plugin)
