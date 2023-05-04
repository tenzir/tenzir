//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>

#include <caf/error.hpp>

#include <filesystem>
#include <system_error>

namespace vast::plugins::directory {

class plugin : public virtual saver_plugin {
  auto initialize(const record&, const record&) -> caf::error override {
    // TODO: option for path output to stdout as file is closed
    saver_plugin_ = plugins::find<saver_plugin>("file");
    if (!saver_plugin_) {
      return caf::make_error(ec::logic_error, "failed to find file saver");
    }
    return caf::none;
  }

  auto make_saver(std::span<std::string const> args, printer_info info,
                  [[maybe_unused]] operator_control_plane& ctrl) const
    -> caf::expected<saver> override {
    if (args.size() != 1) {
      return caf::make_error(ec::syntax_error,
                             "only one argument for directory saver allowed");
    }
    if (!info.input_schema) {
      return caf::make_error(ec::syntax_error,
                             "cannot use directory saver "
                             "outside of write ... to directory");
    }
    auto dir_path = std::filesystem::path(args.front());
    std::error_code ec{};
    std::filesystem::create_directories(dir_path, ec);
    if (ec) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("creating directory {} failed: {}",
                                         dir_path, ec.message()));
    }
    auto file_path
      = dir_path
        / fmt::format("{}.{}.{}", info.input_schema.name(),
                      info.input_schema.make_fingerprint(), info.format);
    auto new_args = std::vector<std::string>{file_path.string()};
    auto file_saver
      = saver_plugin_->make_saver(new_args, std::move(info), ctrl);
    if (not file_saver)
      return std::move(file_saver.error());
    auto guard = caf::detail::make_scope_guard([=] {
      // We also print this when the operator fails at runtime, but then again
      // this also means that we did create the file, so that's probably alright.
      fmt::print(stdout, "{}\n", file_path.string());
    });
    return [file_saver = std::move(*file_saver),
            guard = std::make_shared<decltype(guard)>(std::move(guard))](
             chunk_ptr input) {
      (void)guard;
      // TODO: handle overwrite semantics
      return file_saver(std::move(input));
    };
  }

  auto default_printer([[maybe_unused]] std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"json", {}};
  }

  auto saver_does_joining() const -> bool override {
    return false;
  }

  auto name() const -> std::string override {
    return "directory";
  }

private:
  const saver_plugin* saver_plugin_{nullptr};
};
} // namespace vast::plugins::directory

VAST_REGISTER_PLUGIN(vast::plugins::directory::plugin)
