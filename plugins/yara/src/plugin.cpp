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

#include <deque>
#include <yara.h>

namespace tenzir::plugins::yara {

namespace {

/// Arguments to the operator.
struct operator_args {
  bool blockwise;
  bool compiled_rules;
  bool fast_scan;
  std::vector<std::string> rules;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("blockwise", x.blockwise),
              f.field("compiled_rules", x.compiled_rules),
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
      die(fmt::format("unhandled status value: {}", status));
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
    case ERROR_BLOCK_NOT_READY:
      return caf::make_error(ec::incomplete);
  }
  return {};
}

/// Constructs a sequence of memory blocks that work with the incremental
/// scanning functions that YARA provides.
class memory_block_vector {
public:
  memory_block_vector() noexcept
    : iterator_{
      .context = this,
      .first = first,
      .next = next,
      .file_size = nullptr,
      .last_error = ERROR_SUCCESS,
    } {
  }

  ~memory_block_vector() noexcept = default;

  memory_block_vector(const memory_block_vector&) = delete;
  auto operator=(const memory_block_vector&) -> memory_block_vector& = delete;
  memory_block_vector(memory_block_vector&&) = delete;
  auto operator=(memory_block_vector&&) -> memory_block_vector& = delete;

  /// Adds a new block at the end.
  auto push_back(chunk_ptr chunk) {
    // The *base* of a chunk is its byte offset in the sequence of all chunks
    // seen. This is similar to what YARA does for scanning process memory. See
    // https://github.com/VirusTotal/yara/issues/1356 for a more detailed
    // discussion.
    auto base = uint64_t{0};
    if (not blocks_.empty()) {
      auto& last = blocks_.back().first;
      base = last->base + last->size;
    }
    auto block = std::make_unique<YR_MEMORY_BLOCK>(YR_MEMORY_BLOCK{
      .size = chunk->size(),
      .base = base,
      // The const_cast is needed by the C API and safe because it is only
      // passed through as user context and later casted back to a const
      // uint8_t* in fetch().
      .context = const_cast<std::byte*>(chunk->data()),
      .fetch_data = fetch,
    });
    blocks_.emplace_back(std::move(block), std::move(chunk));
  }

  /// Relinquishes a block of memory from the beginning.
  auto pop_front() -> bool {
    if (blocks_.empty())
      return false;
    blocks_.pop_front();
    --offset_;
    return true;
  }

  /// Signal that no further blocks are being added. This results in the block
  /// iterator returning `ERROR_SUCCESS` instead of `ERROR_BLOCK_NOT_READY`, and
  /// thereby triggering a scan.
  auto done() -> void {
    done_ = true;
  }

  /// Retrieve the underlying block iterator for the YARA API.
  auto iterator() -> YR_MEMORY_BLOCK_ITERATOR* {
    return &iterator_;
  }

private:
  static auto fetch(YR_MEMORY_BLOCK* self) -> const uint8_t* {
    return reinterpret_cast<const uint8_t*>(self->context);
  }

  static auto first(YR_MEMORY_BLOCK_ITERATOR* iterator) -> YR_MEMORY_BLOCK* {
    auto* self = reinterpret_cast<memory_block_vector*>(iterator->context);
    TENZIR_DEBUG("setting iterator to first block");
    self->offset_ = 0;
    return next(iterator);
  }

  static auto next(YR_MEMORY_BLOCK_ITERATOR* iterator) -> YR_MEMORY_BLOCK* {
    auto* self = reinterpret_cast<memory_block_vector*>(iterator->context);
    TENZIR_ASSERT(self->offset_ <= self->blocks_.size());
    if (self->offset_ == self->blocks_.size()) {
      // If we have returned all buffered blocks, we must decide whether we are
      // truly done or whether more blocks are expected.
      TENZIR_DEBUG("reached last block {} (done = {})", self->offset_,
                   self->done_);
      self->iterator_.last_error
        = self->done_ ? ERROR_SUCCESS : ERROR_BLOCK_NOT_READY;
      return nullptr;
    }
    TENZIR_DEBUG("returning next block {} (done = {})", self->offset_,
                 self->done_);
    self->iterator_.last_error = ERROR_SUCCESS;
    return self->blocks_[self->offset_++].first.get();
  }

  YR_MEMORY_BLOCK_ITERATOR iterator_;
  std::deque<std::pair<std::unique_ptr<YR_MEMORY_BLOCK>, chunk_ptr>> blocks_;
  size_t offset_ = 0;
  bool done_ = false;
};

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
    if (auto err = to_error(status))
      return err;
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
    if (status == ERROR_INSUFICIENT_MEMORY)
      return std::nullopt;
    TENZIR_ASSERT(status == ERROR_SUCCESS);
    // Set flags.
    auto flags = 0;
    if (opts.fast_scan)
      flags |= SCAN_FLAGS_FAST_MODE;
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
    if (scanner_)
      yr_scanner_destroy(scanner_);
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
    if (auto err = to_error(status))
      return err;
    return builder.finish_as_table_slice("yara.match");
  }

  /// Checks a sequence of memory blocks for rule matches.
  auto scan(memory_block_vector& blocks)
    -> caf::expected<std::vector<table_slice>> {
    auto builder = series_builder{};
    yr_scanner_set_callback(scanner_, callback, &builder);
    auto status = yr_scanner_scan_mem_blocks(scanner_, blocks.iterator());
    if (auto err = to_error(status))
      return err;
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
        if (meta->type == META_TYPE_INTEGER)
          meta_rec.field(identifier).data(int64_t{meta->integer});
        else if (meta->type == META_TYPE_BOOLEAN)
          meta_rec.field(identifier).data(meta->integer != 0);
        else
          meta_rec.field(identifier).data(std::string_view{meta->string});
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
            auto blob_view
              = std::basic_string_view<std::byte>{bytes.data(), bytes.size()};
            match_rec.field("data").data(blob_view);
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
      die("unhandled message type in YARA callback");
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

  ~compiler() noexcept {
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
          ec::unspecified, fmt::format("got {} error(s) while compiling YARA "
                                       "rule: {}",
                                       num_errors, path));
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
    if (num_errors > 0)
      return caf::make_error(ec::unspecified,
                             fmt::format("got {} error(s) while compiling YARA "
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

  auto operator()(generator<chunk_ptr> input, exec_ctx ctx) const
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
        if (auto err = compiler->add(std::filesystem::path{rule})) {
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
    if (args_.blockwise) {
      for (auto&& chunk : input) {
        if (not chunk) {
          co_yield {};
          continue;
        }
        if (auto slices = scanner->scan(as_bytes(chunk))) {
          for (auto&& slice : *slices)
            co_yield slice;
        } else {
          diagnostic::warning("failed to scan block with YARA rules")
            .hint("{}", slices.error())
            .emit(ctrl.diagnostics());
          co_yield {};
        }
      }
    } else {
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
        for (auto&& slice : *slices)
          co_yield slice;
      } else {
        diagnostic::error("failed to scan blocks with YARA rules")
          .hint("{}", slices.error())
          .emit(ctrl.diagnostics());
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
        else if (arg->inner == "-B" || arg->inner == "--blockwise")
          args.blockwise = true;
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
