//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/pipeline.hpp"

#include <vast/argument_parser.hpp>
#include <vast/diagnostics.hpp>
#include <vast/plugin.hpp>
#include <vast/tql/parser.hpp>

#include <caf/error.hpp>

#include <filesystem>
#include <system_error>

namespace vast::plugins::directory {

class directory_saver final : public plugin_saver {
public:
  directory_saver() = default;

  explicit directory_saver(std::string path) : path_{std::move(path)} {
  }

  auto name() const -> std::string override {
    return "directory";
  }

  auto
  instantiate(operator_control_plane& ctrl, std::optional<printer_info> info)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    if (!info) {
      return caf::make_error(ec::syntax_error,
                             "cannot use directory saver "
                             "outside of write ... to directory");
    }
    auto dir_path = std::filesystem::path(path_);
    std::error_code ec{};
    std::filesystem::create_directories(dir_path, ec);
    if (ec) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("creating directory {} failed: {}",
                                         dir_path, ec.message()));
    }
    auto file_path
      = dir_path
        / fmt::format("{}.{}.{}", info->input_schema.name(),
                      info->input_schema.make_fingerprint(), info->format);
    auto const* p = plugins::find<saver_parser_plugin>("file");
    if (!p) {
      return caf::make_error(ec::unspecified, "could not find `file` saver");
    }
    auto diag = null_diagnostic_handler{};
    // TODO: We should probably use a better mechanism here than escaping and
    // re-parsing.
    auto pi = tql::make_parser_interface(
      escape_operator_arg(file_path.string()), diag);
    auto parsed = std::unique_ptr<plugin_saver>{};
    try {
      parsed = p->parse_saver(*pi);
    } catch (diagnostic& d) {
      return caf::make_error(ec::unspecified, fmt::to_string(d));
    }
    auto file_saver = parsed->instantiate(ctrl, std::move(info));
    if (not file_saver)
      return std::move(file_saver.error());
    auto guard = caf::detail::make_scope_guard([=] {
      // We also print this when the operator fails at runtime, but
      // then again this also means that we did create the file, so
      // that's probably alright.
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

  auto is_joining() const -> bool override {
    return false;
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, directory_saver& x) -> bool {
    return f.apply(x.path_);
  }

private:
  std::string path_;
};

class plugin : public virtual saver_plugin<directory_saver> {
public:
  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{name(), "https://docs.tenzir.com/next/"
                                          "connectors/directory"};
    auto path = std::string{};
    parser.add(path, "<path>");
    parser.parse(p);
    return std::make_unique<directory_saver>(std::move(path));
  }
};

} // namespace vast::plugins::directory

VAST_REGISTER_PLUGIN(vast::plugins::directory::plugin)
