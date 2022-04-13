//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cef/add.hpp"

#include <vast/data.hpp>
#include <vast/detail/assert.hpp>
#include <vast/detail/line_range.hpp>
#include <vast/detail/make_io_stream.hpp>
#include <vast/error.hpp>
#include <vast/format/reader.hpp>
#include <vast/format/single_layout_reader.hpp>
#include <vast/module.hpp>
#include <vast/plugin.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <istream>
#include <memory>

namespace vast::plugins::cef {

// TODO: consider moving into separate file.
class reader : public format::single_layout_reader {
public:
  using super = single_layout_reader;

  /// Constructs a CEF reader.
  /// @param options Additional options.
  /// @param in The stream of JSON objects.
  reader(const caf::settings& options, std::unique_ptr<std::istream> in
                                       = nullptr)
    : super(options) {
    if (in != nullptr)
      reset(std::move(in));
    cef_type_ = type{"cef.event", record_type{
                                    {"cef_version", count_type{}},
                                    {"device_vendor", string_type{}},
                                    {"device_product", string_type{}},
                                    {"device_version", string_type{}},
                                    {"signature_id", string_type{}},
                                    {"name", string_type{}},
                                    {"severity", string_type{}},
                                    {"extension", string_type{}},
                                  }};
  }

  reader(const reader&) = delete;
  reader& operator=(const reader&) = delete;
  reader(reader&&) noexcept = default;
  reader& operator=(reader&&) noexcept = default;
  ~reader() override = default;

  void reset([[maybe_unused]] std::unique_ptr<std::istream> in) override {
    VAST_ASSERT(in != nullptr);
    input_ = std::move(in);
    lines_ = std::make_unique<detail::line_range>(*input_);
  }

  caf::error module(class module new_module) override {
    // TODO: relax this check; congruency is too strong. Rather, the only thing
    // that needs to be congruent is the general shape of the schema, with
    // extensions including room for variability.
    return replace_if_congruent({&cef_type_}, new_module);
  }

  [[nodiscard]] class module module() const override {
    class module result {};
    result.add(cef_type_);
    return result;
  }

  [[nodiscard]] const char* name() const override {
    return "cef-reader";
  }

protected:
  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& cons) override {
    VAST_ASSERT(max_events > 0);
    VAST_ASSERT(max_slice_size > 0);
    VAST_ASSERT(lines_ != nullptr);
    if (builder_ == nullptr) {
      VAST_ASSERT(caf::holds_alternative<record_type>(cef_type_));
      if (!reset_builder(cef_type_))
        return caf::make_error(ec::parse_error, //
                               "unable to create builder for CEF type");
    }
    size_t produced = 0;
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
      const auto& line = lines_->get();
      ++num_lines_;
      if (line.empty()) {
        VAST_DEBUG("{} ignores empty line at {}",
                   detail::pretty_type_name(this), lines_->line_number());
        continue;
      }
      if (auto err = add(line, *builder_)) {
        VAST_WARN("{} failed to parse line {}: {} ({})",
                  detail::pretty_type_name(this), lines_->line_number(), line,
                  err);
        ++num_invalid_lines_;
        // FIXME: make this more resilient to failures by resetting the builder
        // here and continuing.
        // continue;
        return finish(cons, err);
      }
      produced++;
      batch_events_++;
      if (builder_->rows() == max_slice_size)
        if (auto err = finish(cons, caf::none))
          return err;
    }
    return finish(cons, caf::none);
  }

private:
  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  size_t num_invalid_lines_ = 0;
  size_t num_lines_ = 0;
  type cef_type_;
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

  [[nodiscard]] const char* reader_documentation() const override {
    // TODO:
    // - substantiate
    // - add linke to canonical CEF reference
    return R"__(The `import cef` parses ASCII input as [Common Event Format
(CEF)][cef].

Here's an example that reads file with one CEF log per line:

```bash
vast import cef < file.log
```

[cef]: link-to-cef
)__";
  }

  [[nodiscard]] caf::config_option_set
  reader_options(command::opts_builder&& opts) const override {
    // TODO: consider options
    return std::move(opts).add<bool>("tbd", "tbd").finish();
  }

  [[nodiscard]] std::unique_ptr<format::reader>
  make_reader(const caf::settings& options) const override {
    auto in = detail::make_input_stream(options);
    return std::make_unique<reader>(options, in ? std::move(*in) : nullptr);
  }
};

} // namespace vast::plugins::cef

VAST_REGISTER_PLUGIN(vast::plugins::cef::plugin)
