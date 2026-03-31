//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>

#include <arrow/util/utf8.h>
#include <boost/regex.hpp>

namespace tenzir::plugins::read_delimited_regex {

namespace {

using std::chrono::steady_clock;

struct ReadDelimitedRegexArgs {
  located<data> separator;
  Option<bool> binary;
  bool include_separator = false;
};

class ReadDelimitedRegex final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadDelimitedRegex(ReadDelimitedRegexArgs args)
    : args_{std::move(args)} {
    auto pattern = std::string{};
    match(
      args_.separator.inner,
      [&](std::string const& s) {
        pattern = s;
      },
      [&](blob const& b) {
        pattern.assign(reinterpret_cast<char const*>(b.data()), b.size());
      },
      [](auto const&) {
        TENZIR_UNREACHABLE();
      });
    binary_ = args_.binary.unwrap_or(is<blob>(args_.separator.inner));
    expr_ = boost::regex{pattern.data(), pattern.size(),
                         boost::regex_constants::optimize};
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (flush_at_) {
      co_await sleep_until(*flush_at_);
    } else {
      co_await sleep_for(defaults::import::batch_timeout);
    }
    co_return PeriodicTick{};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(result.try_as<PeriodicTick>());
    TENZIR_UNUSED(ctx);
    if (flush_at_ and steady_clock::now() >= *flush_at_) {
      co_await flush(push);
    }
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not input or input->size() == 0) {
      co_return;
    }
    buffer_.append(reinterpret_cast<char const*>(input->data()), input->size());
    match_and_consume(/*has_finished=*/false, ctx);
    // Only flush immediately when the batch is full; small batches at the end
    // of a chunk are held until the periodic tick or finalize().
    if (static_cast<uint64_t>(builder_.length())
        >= defaults::import::table_slice_size) {
      co_await flush(push);
    } else {
      flush_at_ = Option{steady_clock::now() + defaults::import::batch_timeout};
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    match_and_consume(/*has_finished=*/true, ctx);
    co_await flush(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await flush(push);
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("last_match_zero", last_match_zero_);
  }

private:
  /// Runs the regex search loop over `buffer_`, emitting segments and
  /// advancing `last_matched_`. When `has_finished` is false (called from
  /// `process()`), matches that end exactly at the buffer boundary are skipped
  /// — the separator may extend into the next chunk. When `has_finished` is
  /// true (called from `finalize()`), all remaining matches and the final
  /// trailing segment are emitted without that guard.
  auto match_and_consume(bool has_finished, OpCtx& ctx) -> void {
    auto current = buffer_.cbegin();
    // If the last match was zero-width, skip one character before searching
    // again to prevent re-triggering the same zero-width position infinitely.
    auto begin
      = current + (last_match_zero_ and current < buffer_.cend() ? 1 : 0);
    boost::match_results<std::string::const_iterator> what;
    while (begin <= buffer_.cend()
           and boost::regex_search(begin, buffer_.cend(), what, expr_)) {
      // If the match ends at the buffer boundary and input is not yet
      // complete, stop: the separator may extend into the next chunk.
      if (not has_finished and what[0].second == buffer_.cend()) {
        break;
      }
      emit(args_.include_separator ? std::string_view{current, what[0].second}
                                   : std::string_view{current, what[0].first},
           ctx);
      last_match_zero_ = what[0].first == what[0].second;
      current = what[0].second;
      if (last_match_zero_ and current == buffer_.cend()) {
        break;
      }
      begin = current + (last_match_zero_ ? 1 : 0);
    }
    if (has_finished and current < buffer_.cend()) {
      // Emit any trailing data after the last match.
      emit(std::string_view{current, buffer_.cend()}, ctx);
      buffer_.clear();
    } else {
      buffer_ = buffer_.substr(static_cast<size_t>(current - buffer_.cbegin()));
    }
  }

  auto emit(std::string_view segment, OpCtx& ctx) -> void {
    if (binary_) {
      builder_.record().field("data", as_bytes(segment));
    } else {
      if (not arrow::util::ValidateUTF8(segment)) {
        diagnostic::warning("got invalid UTF-8")
          .hint("use `binary=true` if you are reading binary data")
          .emit(ctx);
        return;
      }
      builder_.record().field("data", segment);
    }
  }

  auto flush(Push<table_slice>& push) -> Task<void> {
    if (builder_.length() > 0) {
      co_await push(builder_.finish_assert_one_slice("tenzir.data"));
    }
    flush_at_ = None{};
  }

  struct PeriodicTick {};

  ReadDelimitedRegexArgs args_;
  boost::regex expr_;
  bool binary_ = false;
  std::string buffer_;
  // True when last match had zero width.
  // In this case, we should skip the next char, so
  // we don't match at this position again.
  bool last_match_zero_ = false;
  series_builder builder_;
  Option<steady_clock::time_point> flush_at_ = None{};
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_delimited_regex";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadDelimitedRegexArgs, ReadDelimitedRegex>{};
    auto sep = d.positional("separator", &ReadDelimitedRegexArgs::separator,
                            "string|blob");
    d.named("binary", &ReadDelimitedRegexArgs::binary);
    d.named("include_separator", &ReadDelimitedRegexArgs::include_separator);
    d.validate([sep](DescribeCtx& ctx) -> Empty {
      TRY(auto s, ctx.get(sep));
      if (not is<std::string>(s.inner) and not is<blob>(s.inner)) {
        diagnostic::error("separator must be a `string` or `blob`")
          .primary(s.source)
          .emit(ctx);
        return {};
      }
      auto pattern = std::string{};
      if (is<std::string>(s.inner)) {
        pattern = as<std::string>(s.inner);
      } else {
        auto const& b = as<blob>(s.inner);
        pattern.assign(reinterpret_cast<char const*>(b.data()), b.size());
      }
      if (pattern.empty()) {
        diagnostic::error("separator must not be empty")
          .primary(s.source)
          .emit(ctx);
        return {};
      }
      try {
        boost::regex{pattern.data(), pattern.size(),
                     boost::regex_constants::optimize};
      } catch (std::exception const& e) {
        diagnostic::error("invalid regex: {}", e.what())
          .primary(s.source)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_delimited_regex

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_delimited_regex::plugin)
