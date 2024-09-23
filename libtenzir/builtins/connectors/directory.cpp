//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql/parser.hpp>

#include <caf/error.hpp>

#include <filesystem>
#include <system_error>

namespace tenzir::plugins::directory {

struct saver_args {
  std::string path;
  bool append;
  bool real_time;

  template <class Inspector>
  friend auto inspect(Inspector& f, saver_args& x) -> bool {
    return f.object(x)
      .pretty_name("saver_args")
      .fields(f.field("path", x.path), f.field("append", x.append),
              f.field("real_time", x.real_time));
  }
};

class directory_saver final : public plugin_saver {
public:
  directory_saver() = default;

  explicit directory_saver(saver_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "directory";
  }

  auto instantiate(exec_ctx ctx, std::optional<printer_info> info)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    if (!info) {
      return caf::make_error(ec::syntax_error,
                             "cannot use directory saver outside of `to "
                             "directory write ...`");
    }
    auto dir_path = std::filesystem::path(args_.path);
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
    auto file_pipeline = escape_operator_arg(file_path.string());
    if (args_.append)
      file_pipeline += " --append";
    if (args_.real_time)
      file_pipeline += " --real-time";
    // TODO: We should probably use a better mechanism here than escaping and
    // re-parsing.
    auto pi = tql::make_parser_interface(std::move(file_pipeline), diag);
    auto parsed = std::unique_ptr<plugin_saver>{};
    try {
      parsed = p->parse_saver(*pi);
    } catch (diagnostic& d) {
      return caf::make_error(ec::unspecified, fmt::format("{:?}", d));
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
    return f.apply(x.args_);
  }

private:
  saver_args args_;
};

class plugin : public virtual saver_plugin<directory_saver> {
public:
  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{name(), "https://docs.tenzir.com/"
                                          "connectors/directory"};
    auto args = saver_args{};
    parser.add(args.path, "<path>");
    parser.add("-a,--append", args.append);
    parser.add("-r,--real-time", args.real_time);
    parser.parse(p);
    return std::make_unique<directory_saver>(std::move(args));
  }
};

} // namespace tenzir::plugins::directory

TENZIR_REGISTER_PLUGIN(tenzir::plugins::directory::plugin)
