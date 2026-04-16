//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>
#include <tenzir/view.hpp>

#include <yara/libyara.h>
#include <yara/types.h>

#include <string_view>
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

/// Translates a YARA status code to an error.
auto to_error(int status) -> caf::error {
  switch (status) {
    default:
      TENZIR_UNREACHABLE();
    case ERROR_SUCCESS:
      break;
    case ERROR_INSUFFICIENT_MEMORY:
      return caf::make_error(ec::unspecified,
                             "insufficient memory to load rule");
    case ERROR_COULD_NOT_ATTACH_TO_PROCESS:
      return caf::make_error(ec::unspecified, "could not attach to process");
    case ERROR_COULD_NOT_OPEN_FILE:
      return caf::make_error(ec::unspecified, "could not open file");
    case ERROR_COULD_NOT_MAP_FILE:
      return caf::make_error(ec::unspecified, "could not mmap file");
    case ERROR_INVALID_FILE:
      return caf::make_error(ec::unspecified, "invalid YARA rule");
    case ERROR_CORRUPT_FILE:
      return caf::make_error(ec::unspecified, "corrupt YARA rule");
    case ERROR_UNSUPPORTED_FILE_VERSION:
      return caf::make_error(ec::unspecified, "unsupported YARA file version");
    case ERROR_TOO_MANY_SCAN_THREADS:
      return caf::make_error(ec::unspecified, "too many scan threads");
    case ERROR_SCAN_TIMEOUT:
      return caf::make_error(ec::unspecified, "scan timeout");
    case ERROR_CALLBACK_ERROR:
      return caf::make_error(ec::unspecified, "callback error");
    case ERROR_TOO_MANY_MATCHES:
      return caf::make_error(ec::unspecified, "too many matches");
  }
  return {};
}

/// A set of YARA rules.
class rules {
  friend class compiler;
  friend class scanner;

public:
  /// Loads a compiled rule.
  /// @param filename The path to the rule file.
  static auto load(const std::string& filename) -> caf::expected<rules> {
    auto result = rules{};
    auto status = yr_rules_load(filename.c_str(), &result.rules_);
    if (auto err = to_error(status); err.valid()) {
      return err;
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
    if (rules_) {
      yr_rules_destroy(rules_);
    }
  }

  rules(const rules&) = delete;
  auto operator=(const rules&) -> rules& = delete;

private:
  rules() = default;
  YR_RULES* rules_ = nullptr;
};

/// A YARA rule scanner.
class scanner {
public:
  static auto make(const rules& rules, scan_options opts = {})
    -> std::optional<scanner> {
    // Create scanner from rules.
    auto result = scanner{std::move(opts)};
    auto status = yr_scanner_create(rules.rules_, &result.scanner_);
    if (status == ERROR_INSUFICIENT_MEMORY) {
      return std::nullopt;
    }
    TENZIR_ASSERT(status == ERROR_SUCCESS);
    // Set flags.
    auto flags = 0;
    if (opts.fast_scan) {
      flags |= SCAN_FLAGS_FAST_MODE;
    }
    yr_scanner_set_flags(result.scanner_, flags);
    // Set timeout.
    auto timeout = detail::narrow_cast<int>(opts.timeout.count());
    yr_scanner_set_timeout(result.scanner_, timeout);
    return result;
  }

  scanner(scanner&& other) noexcept : scanner_{other.scanner_} {
    other.scanner_ = nullptr;
  }

  auto operator=(scanner&& other) noexcept -> scanner& {
    std::swap(scanner_, other.scanner_);
    return *this;
  }

  ~scanner() noexcept {
    if (scanner_) {
      yr_scanner_destroy(scanner_);
    }
  }

  scanner(const scanner&) = delete;
  auto operator=(const scanner&) -> scanner& = delete;

  /// Performs a one-shot scan of a given block of memory.
  auto scan(std::span<const std::byte> bytes)
    -> caf::expected<std::vector<table_slice>> {
    auto buffer = reinterpret_cast<const uint8_t*>(bytes.data());
    auto buffer_size = bytes.size();
    auto builder = series_builder{};
    yr_scanner_set_callback(scanner_, callback, &builder);
    auto status = yr_scanner_scan_mem(scanner_, buffer, buffer_size);
    if (auto err = to_error(status); err.valid()) {
      return err;
    }
    return builder.finish_as_table_slice("yara.match");
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
      auto rec = row.field("rule").record();
      rec.field("identifier").data(rule->identifier);
      rec.field("namespace").data(std::string_view{rule->ns->name});
      const char* tag = nullptr;
      auto tags = rec.field("tags").list();
      yr_rule_tags_foreach(rule, tag) {
        tags.data(std::string_view{tag});
      }
      auto meta_rec = rec.field("meta").record();
      YR_META* meta = nullptr;
      yr_rule_metas_foreach(rule, meta) {
        auto identifier = std::string_view{meta->identifier};
        if (meta->type == META_TYPE_INTEGER) {
          meta_rec.field(identifier).data(int64_t{meta->integer});
        } else if (meta->type == META_TYPE_BOOLEAN) {
          meta_rec.field(identifier).data(meta->integer != 0);
        } else {
          meta_rec.field(identifier).data(std::string_view{meta->string});
        }
      }
      // First we bring all strings to the attention of the user. This is
      // valuable rule context in case the rule is not immediately handly.
      auto strings = rec.field("strings").record();
      YR_STRING* string = nullptr;
      yr_rule_strings_foreach(rule, string) {
        // TODO: should this be byte?
        auto rule_string
          = std::string_view{reinterpret_cast<const char*>(string->string),
                             detail::narrow_cast<size_t>(string->length)};
        strings.field(string->identifier).data(rule_string);
      }
      // Second we go through the subset of strings that have matches.
      auto matches = row.field("matches").record();
      string = nullptr;
      yr_rule_strings_foreach(rule, string) {
        if (context->matches[string->idx].head != nullptr) {
          auto list = matches.field(string->identifier).list();
          YR_MATCH* match = nullptr;
          yr_string_matches_foreach(context, string, match) {
            auto match_rec = list.record();
            auto bytes = std::span<const std::byte>{
              reinterpret_cast<const std::byte*>(match->data),
              detail::narrow_cast<size_t>(match->data_length)};
            match_rec.field("data").data(blob_view{bytes.data(), bytes.size()});
            match_rec.field("base").data(match->base);
            match_rec.field("offset").data(match->offset);
            match_rec.field("match_length")
              .data(detail::narrow_cast<uint64_t>(match->match_length));
            // TODO: Once we can upgrade to newer versions of libyara, uncomment
            // the line below. YR_MATCH::xor_key is not available in the version
            // we get on Debian.
            // match_rec.field("xor_key").data(uint64_t{match->xor_key});
          }
        }
      }
    } else if (message == CALLBACK_MSG_RULE_NOT_MATCHING) {
      auto* rule = reinterpret_cast<YR_RULE*>(message_data);
      TENZIR_DEBUG("got no match for rule {}", rule->identifier);
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
      TENZIR_UNREACHABLE();
    }
    return CALLBACK_CONTINUE;
  }

  scanner(scan_options opts) : opts_{std::move(opts)} {
  }

  YR_SCANNER* scanner_ = nullptr;
  scan_options opts_;
};

/// Compiles YARA rules.
class compiler {
public:
  /// Constructs a compiler.
  static auto make() -> std::optional<compiler> {
    auto result = compiler{};
    auto status = yr_compiler_create(&result.compiler_);
    if (status == ERROR_INSUFICIENT_MEMORY) {
      return std::nullopt;
    }
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

  ~compiler() noexcept {
    if (compiler_) {
      yr_compiler_destroy(compiler_);
    }
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
        if (auto err = add(entry.path()); err.valid()) {
          return err;
        }
      }
    } else {
      auto* file = std::fopen(path.string().c_str(), "r");
      if (not file) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("failed to open file: {}", path));
      }
      auto name_space = nullptr;
      auto num_errors = yr_compiler_add_file(compiler_, file, name_space,
                                             path.string().c_str());
      auto result = std::fclose(file);
      (void)result;
      if (num_errors > 0) {
        return caf::make_error(
          ec::unspecified, fmt::format("got {} error(s) while compiling YARA "
                                       "rule: {}",
                                       num_errors, path));
      }
    }
    return {};
  }

  /// Adds a string representation of a YARA rule.
  /// @param str The rule.
  /// @returns An error upon failure.
  auto add(const std::string& str) -> caf::error {
    auto name_space = nullptr;
    auto num_errors
      = yr_compiler_add_string(compiler_, str.c_str(), name_space);
    if (num_errors > 0) {
      return caf::make_error(ec::unspecified,
                             fmt::format("got {} error(s) while compiling YARA "
                                         "rule: '{}'",
                                         num_errors, str));
    }
    return {};
  }

  /// Compiles the added set of rules.
  /// @warning You cannot add rules afterwards.
  /// @returns The set of compiled rules.
  auto compile() -> caf::expected<rules> {
    YR_RULES* yr_rules = nullptr;
    auto status = yr_compiler_get_rules(compiler_, &yr_rules);
    if (status == ERROR_INSUFFICIENT_MEMORY) {
      return caf::make_error(ec::unspecified,
                             "insufficent memory to compile rules");
    }
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
      diagnostic::error("insufficient memory to create YARA compiler")
        .emit(ctrl.diagnostics());
      co_return;
    }
    if (args_.compiled_rules) {
      TENZIR_ASSERT(args_.rules.size() == 1);
      rules = rules::load(args_.rules[0]);
      if (not rules) {
        diagnostic::error("failed to load compiled YARA rules")
          .note("{}", rules.error())
          .emit(ctrl.diagnostics());
        co_return;
      }
    } else {
      for (const auto& rule : args_.rules) {
        if (auto err = compiler->add(std::filesystem::path{rule});
            err.valid()) {
          diagnostic::error("failed to add YARA rule to compiler")
            .note("rule: {}", rule)
            .note("error: {}", err)
            .emit(ctrl.diagnostics());
          co_return;
        }
      }
      rules = compiler->compile();
    }
    auto opts = scan_options{
      .fast_scan = args_.fast_scan,
    };
    auto scanner = scanner::make(*rules, opts);
    if (not scanner) {
      diagnostic::warning("failed to construct YARA scanner")
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Small optimization: in case the entire input consists of a single
    // chunk, we don't want to copy it at all. This actually may happen
    // frequently when memory-mapping files, so it's worthwhile addressing.
    auto first = chunk_ptr{};
    std::vector<std::byte> buffer;
    for (auto&& chunk : input) {
      if (not chunk) {
        co_yield {};
        continue;
      }
      if (not buffer.empty()) {
        buffer.insert(buffer.end(), chunk->begin(), chunk->end());
      } else if (not first) {
        first = chunk;
      } else {
        buffer.reserve(first->size() + chunk->size());
        buffer.insert(buffer.end(), first->begin(), first->end());
        buffer.insert(buffer.end(), chunk->begin(), chunk->end());
      }
    }
    auto bytes = buffer.empty() ? as_bytes(first) : as_bytes(buffer);
    if (auto slices = scanner->scan(bytes)) {
      for (auto&& slice : *slices) {
        co_yield slice;
      }
    } else {
      diagnostic::error("failed to scan input with YARA rules")
        .hint("{}", slices.error())
        .emit(ctrl.diagnostics());
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

struct YaraArgs {
  located<list> rules = {};
  bool compiled_rules = false;
  bool fast_scan = false;
};

class Yara final : public Operator<chunk_ptr, table_slice> {
public:
  explicit Yara(YaraArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (failed_) {
      co_return;
    }
    failed_ = not initialize(ctx.dh());
    co_return;
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push, ctx);
    if (failed_ or not scanner_ or not input or input->size() == 0) {
      co_return;
    }
    buffered_chunks_.push_back(std::move(input));
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (failed_ or not scanner_) {
      co_return FinalizeBehavior::done;
    }
    auto joined = join_chunks(buffered_chunks_);
    buffered_chunks_.clear();
    if (not joined) {
      co_return FinalizeBehavior::done;
    }
    auto slices = scanner_->scan(as_bytes(joined));
    if (not slices) {
      diagnostic::error("failed to scan input with YARA rules")
        .hint("{}", slices.error())
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    for (auto& slice : *slices) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& serde) -> void override {
    // We intentionally serialize buffered input because the operator must see
    // the entire byte stream before emitting matches.
    serde("failed", failed_);
    serde("buffered_chunks", buffered_chunks_);
  }

  auto state() -> OperatorState override {
    return failed_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  static auto join_chunks(std::vector<chunk_ptr> const& chunks) -> chunk_ptr {
    // YARA matches may span chunk boundaries, so the operator scans a
    // contiguous byte sequence. Keep the single-chunk fast path to avoid an
    // unnecessary copy.
    auto first = chunk_ptr{};
    auto buffer = std::vector<std::byte>{};
    for (auto const& chunk : chunks) {
      if (not chunk or chunk->size() == 0) {
        continue;
      }
      if (not buffer.empty()) {
        buffer.insert(buffer.end(), chunk->begin(), chunk->end());
      } else if (not first) {
        first = chunk;
      } else {
        buffer.reserve(first->size() + chunk->size());
        buffer.insert(buffer.end(), first->begin(), first->end());
        buffer.insert(buffer.end(), chunk->begin(), chunk->end());
      }
    }
    if (buffer.empty()) {
      return first;
    }
    return chunk::make(std::move(buffer));
  }

  auto materialize_rules(diagnostic_handler& dh) const
    -> Option<std::vector<std::string>> {
    auto result = std::vector<std::string>{};
    result.reserve(args_.rules.inner.size());
    for (auto const& rule : args_.rules.inner) {
      if (not is<std::string>(rule)) {
        diagnostic::error("expected type string for rule")
          .primary(args_.rules)
          .emit(dh);
        return {};
      }
      result.push_back(as<std::string>(rule));
    }
    if (result.empty()) {
      diagnostic::error("no rules provided").emit(dh);
      return {};
    }
    if (args_.compiled_rules and result.size() > 1) {
      diagnostic::error("can't accept multiple rules in compiled form")
        .primary(args_.rules)
        .hint("provide exactly one rule argument")
        .emit(dh);
      return {};
    }
    return result;
  }

  auto initialize(diagnostic_handler& dh) -> bool {
    auto rule_strings = materialize_rules(dh);
    if (not rule_strings) {
      return false;
    }
    auto compiler = compiler::make();
    if (not compiler) {
      diagnostic::error("insufficient memory to create YARA compiler").emit(dh);
      return false;
    }
    if (args_.compiled_rules) {
      TENZIR_ASSERT(rule_strings->size() == 1);
      auto loaded = rules::load((*rule_strings)[0]);
      if (not loaded) {
        diagnostic::error("failed to load compiled YARA rules")
          .note("{}", loaded.error())
          .emit(dh);
        return false;
      }
      rules_ = std::move(*loaded);
    } else {
      for (auto const& rule : *rule_strings) {
        if (auto err = compiler->add(std::filesystem::path{rule});
            err.valid()) {
          diagnostic::error("failed to add YARA rule to compiler")
            .note("rule: {}", rule)
            .note("error: {}", err)
            .emit(dh);
          return false;
        }
      }
      auto compiled = compiler->compile();
      if (not compiled) {
        diagnostic::error("failed to compile YARA rules")
          .note("error: {}", compiled.error())
          .emit(dh);
        return false;
      }
      rules_ = std::move(*compiled);
    }
    auto opts = scan_options{
      .fast_scan = args_.fast_scan,
    };
    auto scanner_instance = scanner::make(*rules_, opts);
    if (not scanner_instance) {
      diagnostic::warning("failed to construct YARA scanner").emit(dh);
      return false;
    }
    scanner_ = std::move(*scanner_instance);
    return true;
  }

  YaraArgs args_;
  bool failed_ = false;
  std::vector<chunk_ptr> buffered_chunks_;
  Option<rules> rules_;
  Option<scanner> scanner_;
};

/// The `yara` plugin.
class plugin final : public virtual operator_plugin<yara_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  plugin() {
    const auto ok = yr_initialize();
    TENZIR_ASSERT(ok == ERROR_SUCCESS, "failed to initialize yara");
  }

  ~plugin() final {
    yr_finalize();
  }

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto describe() const -> Description override {
    auto d = Describer<YaraArgs, Yara>{};
    auto rules = d.positional("rules", &YaraArgs::rules, "list<string>");
    auto compiled_rules = d.named("compiled_rules", &YaraArgs::compiled_rules);
    d.named("fast_scan", &YaraArgs::fast_scan);
    d.validate([rules, compiled_rules](DescribeCtx& ctx) -> Empty {
      TRY(auto value, ctx.get(rules));
      auto count = size_t{0};
      for (auto const& rule : value.inner) {
        if (not is<std::string>(rule)) {
          diagnostic::error("expected type string for rule")
            .primary(value)
            .emit(ctx);
          return {};
        }
        ++count;
      }
      if (count == 0) {
        diagnostic::error("no rules provided").emit(ctx);
        return {};
      }
      if (ctx.get(compiled_rules).value_or(false) and count > 1) {
        diagnostic::error("can't accept multiple rules in compiled form")
          .primary(value)
          .hint("provide exactly one rule argument")
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = operator_args{};
    auto rules = located<list>{};
    argument_parser2::operator_("yara")
      .positional("rules", rules, "list<string>")
      .named("compiled_rules", args.compiled_rules)
      .named("fast_scan", args.fast_scan)
      .parse(inv, ctx)
      .ignore();
    for (const auto& rule : rules.inner) {
      if (not is<std::string>(rule)) {
        diagnostic::error("expected type string for rule")
          .primary(rules)
          .emit(ctx);
        return failure::promise();
      }
      args.rules.push_back(std::move(as<std::string>(rule)));
    }
    if (args.rules.empty()) {
      diagnostic::error("no rules provided").emit(ctx);
      return failure::promise();
    }
    if (args.compiled_rules and args.rules.size() > 1) {
      diagnostic::error("can't accept multiple rules in compiled form")
        .primary(rules)
        .hint("provide exactly one rule argument")
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<yara_operator>(std::move(args));
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto args = operator_args{};
    while (auto arg = p.accept_shell_arg()) {
      if (arg) {
        if (arg->inner == "-C" or arg->inner == "--compiled-rules") {
          args.compiled_rules = true;
        } else if (arg->inner == "-f" or arg->inner == "--fast-scan") {
          args.fast_scan = true;
        } else {
          args.rules.push_back(std::move(arg->inner));
        }
      }
    }
    if (args.rules.empty()) {
      diagnostic::error("no rules provided").throw_();
    }
    if (args.compiled_rules and args.rules.size() != 1) {
      diagnostic::error("can't accept multiple rules in compiled form")
        .hint("provide exactly one rule argument")
        .throw_();
    }
    return std::make_unique<yara_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::yara

TENZIR_REGISTER_PLUGIN(tenzir::plugins::yara::plugin)
