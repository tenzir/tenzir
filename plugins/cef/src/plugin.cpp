//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cef/parse.hpp"

#include <vast/data.hpp>
#include <vast/error.hpp>
#include <vast/format/reader.hpp>
#include <vast/format/single_layout_reader.hpp>
#include <vast/module.hpp>
#include <vast/plugin.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

namespace vast::plugins::cef {

// TODO: consider moving into separate file.
class reader : public format::single_layout_reader {
public:
  using super = single_layout_reader;

  /// Constructs a CEF reader.
  /// @param options Additional options.
  explicit reader(const caf::settings& options) : super(options) {
    // TODO: parse options
  }

  reader(const reader&) = delete;
  reader& operator=(const reader&) = delete;
  reader(reader&&) noexcept = default;
  reader& operator=(reader&&) noexcept = default;
  ~reader() override = default;

  void reset([[maybe_unused]] std::unique_ptr<std::istream> in) override {
    // TODO: implement
  }

  caf::error module(class module) override {
    // TODO: implement
    // return replace_if_congruent({&cef_type_}, new_module);
    return caf::none;
  }

  [[nodiscard]] class module module() const override {
    // TODO: implement
    class module result {};
    // result.add(cef_type_);
    return result;
  }

  [[nodiscard]] const char* name() const override {
    return "cef-reader";
  }

protected:
  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& f) override {
    // TODO: implement
    return caf::none;
  }

private:
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
    return std::make_unique<reader>(options);
  }
};

} // namespace vast::plugins::cef

VAST_REGISTER_PLUGIN(vast::plugins::cef::plugin)
