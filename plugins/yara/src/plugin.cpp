//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/die.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

#include <yara.h>

namespace tenzir::plugins::yara {

namespace {

/// Arguments to the operator.
struct operator_args {
  bool compiled_rules;
  bool fast_scan;
  std::vector<std::string> rules;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("compiled_rules", x.compiled_rules),
              f.field("fast_scan", x.fast_scan), f.field("rules", x.rules));
  }
};

/// Options to pass to rules::scan() that affect the scanning behavior.
struct scan_options {
  bool fast_scan{false};
  std::chrono::seconds timeout{1'000'000};
};

/// A set of Yara rules.
class rules {
  friend class compiler;

public:
  /// Loads a compiled rule.
  /// @param filename The path to the rule file.
  static auto load(const std::string& filename) -> caf::expected<rules> {
    auto result = rules{};
    switch (yr_rules_load(filename.c_str(), &result.rules_)) {
      default:
        die("unhandled return value of yr_rules_load");
      case ERROR_SUCCESS:
        break;
      case ERROR_INSUFFICIENT_MEMORY:
        return caf::make_error(ec::unspecified,
                               "insufficient memory to load rule");
      case ERROR_COULD_NOT_OPEN_FILE:
        return caf::make_error(ec::unspecified, "failed to open Yara rule");
      case ERROR_INVALID_FILE:
        return caf::make_error(ec::unspecified, "invalid Yara rule");
      case ERROR_CORRUPT_FILE:
        return caf::make_error(ec::unspecified, "corrupt Yara rule");
      case ERROR_UNSUPPORTED_FILE_VERSION:
        return caf::make_error(ec::unspecified,
                               "unsupported Yara file version");
    }
    return result;
  }

  rules(rules&& other) noexcept : rules_{other.rules_} {
    other.rules_ = nullptr;
  }

  auto operator=(rules&& other) noexcept -> rules& {
    std::swap(rules_, other.rules_);
    return *this;
  }

  ~rules() {
    if (rules_)
      yr_rules_destroy(rules_);
  }

  rules(const rules&) = delete;
  auto operator=(const rules&) -> rules& = delete;

  /// Scans a buffer of bytes.
  auto scan(std::span<const std::byte> bytes, scan_options opts)
    -> caf::expected<std::vector<table_slice>> {
    auto flags = 0;
    if (opts.fast_scan)
      flags |= SCAN_FLAGS_FAST_MODE;
    auto buffer = reinterpret_cast<const uint8_t*>(bytes.data());
    auto buffer_size = bytes.size();
    auto timeout = detail::narrow_cast<int>(opts.timeout.count());
    auto builder = series_builder{};
    auto status = yr_rules_scan_mem(rules_, buffer, buffer_size, flags,
                                    callback, &builder, timeout);
    switch (status) {
      default:
        die("unhandled result value for yr_rules_scan_mem");
      case ERROR_SUCCESS:
        break;
      case ERROR_INSUFFICIENT_MEMORY:
        return caf::make_error(ec::unspecified,
                               "insufficient memory to scan bytes");
      case ERROR_TOO_MANY_SCAN_THREADS:
        return caf::make_error(ec::unspecified, "too many scan threads");
      case ERROR_SCAN_TIMEOUT:
        return caf::make_error(ec::unspecified, "scan timeout");
      case ERROR_CALLBACK_ERROR:
        return caf::make_error(ec::unspecified, "callback error");
      case ERROR_TOO_MANY_MATCHES:
        return caf::make_error(ec::unspecified, "too many matches");
    }
    return builder.finish_as_table_slice("tenzir.yara");
  }

private:
  static auto callback(YR_SCAN_CONTEXT* context, int message,
                       void* message_data, void* user_data) -> int {
    TENZIR_ASSERT(user_data != nullptr);
    auto* builder = reinterpret_cast<series_builder*>(user_data);
    if (message == CALLBACK_MSG_RULE_MATCHING) {
      auto* rule = reinterpret_cast<YR_RULE*>(message_data);
      TENZIR_DEBUG("got a match for rule {}", rule->identifier);
      auto row = builder->record();
      row.field("match").data(true);
    } else if (message == CALLBACK_MSG_RULE_NOT_MATCHING) {
      auto* rule = reinterpret_cast<YR_RULE*>(message_data);
      TENZIR_DEBUG("got no match for rule {}", rule->identifier);
      auto row = builder->record();
      row.field("match").data(false);
    } else if (message == CALLBACK_MSG_IMPORT_MODULE) {
      auto* module = reinterpret_cast<YR_MODULE_IMPORT*>(message_data);
      TENZIR_DEBUG("importing module: {}", module->module_name);
    } else if (message == CALLBACK_MSG_MODULE_IMPORTED) {
      auto* object = reinterpret_cast<YR_OBJECT_STRUCTURE*>(message_data);
      TENZIR_DEBUG("imported module: {}", object->identifier);
    } else if (message == CALLBACK_MSG_TOO_MANY_MATCHES) {
      auto* yr_string = reinterpret_cast<YR_STRING*>(message_data);
      auto string = std::string_view{
        reinterpret_cast<char*>(yr_string->string),
        detail::narrow_cast<std::string_view::size_type>(yr_string->length)};
      TENZIR_WARN("too many matches for string: {}", string);
    } else if (message == CALLBACK_MSG_CONSOLE_LOG) {
      auto* str = reinterpret_cast<char*>(message_data);
      TENZIR_DEBUG("{}", str);
    } else if (message == CALLBACK_MSG_SCAN_FINISHED) {
      TENZIR_DEBUG("completed scan");
    } else {
      die("unhandled message type in Yara callback");
    }
    return CALLBACK_CONTINUE;
  }

  rules() = default;
  YR_RULES* rules_ = nullptr;
};

/// Compiles Yara rules.
class compiler {
public:
  /// Constructs a compiler.
  static auto make() -> std::optional<compiler> {
    auto result = compiler{};
    auto status = yr_compiler_create(&result.compiler_);
    if (status == ERROR_INSUFICIENT_MEMORY)
      return std::nullopt;
    TENZIR_ASSERT(status == ERROR_SUCCESS);
    return result;
  }

  compiler(compiler&& other) noexcept : compiler_{other.compiler_} {
    other.compiler_ = nullptr;
  }

  auto operator=(compiler&& other) noexcept -> compiler& {
    std::swap(compiler_, other.compiler_);
    return *this;
  }

  ~compiler() {
    if (compiler_)
      yr_compiler_destroy(compiler_);
  }

  compiler(const compiler&) = delete;
  auto operator=(const compiler&) -> compiler& = delete;

  /// Adds a single rule or directory of rule.
  /// @param path The path to the rule file/directory.
  /// @returns An error upon failure.
  auto add(const std::filesystem::path& path) -> caf::error {
    if (std::filesystem::is_directory(path)) {
      for (const std::filesystem::directory_entry& entry :
           std::filesystem::recursive_directory_iterator(path)) {
        if (auto err = add(entry.path()))
          return err;
      }
    } else {
      auto* file = std::fopen(path.string().c_str(), "r");
      if (not file)
        return caf::make_error(ec::filesystem_error,
                               fmt::format("failed to open file: {}", path));
      auto name_space = nullptr;
      auto num_errors = yr_compiler_add_file(compiler_, file, name_space,
                                             path.string().c_str());
      auto result = std::fclose(file);
      (void)result;
      if (num_errors > 0)
        return caf::make_error(
          ec::unspecified, fmt::format("got {} error(s) while compiling Yara "
                                       "rule: {}",
                                       num_errors, path));
    }
    return {};
  }

  /// Adds a string representation of a Yara rule.
  /// @param str The rule.
  /// @returns An error upon failure.
  auto add(const std::string& str) -> caf::error {
    auto name_space = nullptr;
    auto num_errors
      = yr_compiler_add_string(compiler_, str.c_str(), name_space);
    if (num_errors > 0)
      return caf::make_error(ec::unspecified,
                             fmt::format("got {} error(s) while compiling Yara "
                                         "rule: '{}'",
                                         num_errors, str));
    return {};
  }

  /// Compiles the added set of rules.
  /// @warning You cannot add rules afterwards.
  /// @returns The set of compiled rules.
  auto compile() -> caf::expected<rules> {
    YR_RULES* yr_rules = nullptr;
    auto status = yr_compiler_get_rules(compiler_, &yr_rules);
    if (status == ERROR_INSUFFICIENT_MEMORY)
      return caf::make_error(ec::unspecified,
                             "insufficent memory to compile rules");
    TENZIR_ASSERT(status == ERROR_SUCCESS);
    auto result = rules{};
    result.rules_ = yr_rules;
    return result;
  }

private:
  compiler() = default;

  YR_COMPILER* compiler_ = nullptr;
};

/// The `yara` operator implementation.
class yara_operator final : public crtp_operator<yara_operator> {
public:
  yara_operator() = default;

  explicit yara_operator(operator_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto rules = caf::expected<class rules>{caf::error{}};
    auto compiler = compiler::make();
    if (not compiler) {
      diagnostic::error("insufficient memory to create Yara compiler")
        .emit(ctrl.diagnostics());
      co_return;
    }
    if (args_.compiled_rules) {
      TENZIR_ASSERT(args_.rules.size() == 1);
      rules = rules::load(args_.rules[0]);
      if (not rules) {
        diagnostic::error("failed to load compiled Yara rules")
          .note("{}", rules.error())
          .emit(ctrl.diagnostics());
        co_return;
      }
    } else {
      for (const auto& rule : args_.rules) {
        if (auto err = compiler->add(std::filesystem::path{rule})) {
          diagnostic::error("failed to add Yara rule to compiler")
            .note("rule: {}", rule)
            .note("error: {}", err)
            .emit(ctrl.diagnostics());
          co_return;
        }
      }
      rules = compiler->compile();
    }
    for (auto&& chunk : input) {
      if (not chunk) {
        co_yield {};
        continue;
      }
      auto opts = scan_options{
        .fast_scan = args_.fast_scan,
      };
      if (auto slices = rules->scan(as_bytes(chunk), opts)) {
        for (auto&& slice : *slices)
          co_yield slice;
      } else {
        diagnostic::warning("failed to scan bytes with Yara rules")
          .hint("{}", slices.error())
          .emit(ctrl.diagnostics());
        co_yield {};
      }
    }
  }

  auto name() const -> std::string override {
    return "yara";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, yara_operator& x) -> bool {
    return f.object(x)
      .pretty_name("yara_operator")
      .fields(f.field("args", x.args_));
  }

private:
  operator_args args_ = {};
};

/// The `yara` plugin.
class plugin final : public virtual operator_plugin<yara_operator> {
public:
  plugin() {
    if (yr_initialize() != ERROR_SUCCESS)
      die("failed to initialize yara");
  }

  ~plugin() final {
    yr_finalize();
  }

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto args = operator_args{};
    while (auto arg = p.accept_shell_arg()) {
      if (arg) {
        if (arg->inner == "-C" || arg->inner == "--compiled-rules")
          args.compiled_rules = true;
        else if (arg->inner == "-f" || arg->inner == "--fast-scan")
          args.fast_scan = true;
        else
          args.rules.push_back(std::move(arg->inner));
      }
    }
    if (args.rules.empty())
      diagnostic::error("no rules provided").throw_();
    if (args.compiled_rules && args.rules.size() != 1)
      diagnostic::error("can't accept multiple rules in compiled form")
        .hint("provide exactly one rule argument")
        .throw_();
    return std::make_unique<yara_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::yara

TENZIR_REGISTER_PLUGIN(tenzir::plugins::yara::plugin)
