//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/pusher.hpp>
#include <tenzir/detail/syslog.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/ast.hpp>

#include <algorithm>
#include <cctype>
#include <limits>
#include <string_view>

namespace tenzir::plugins::read_syslog {

namespace {

namespace syslog = tenzir::plugins::syslog;

auto make_dh(diagnostic_handler& dh, location operator_loc)
  -> transforming_diagnostic_handler {
  return transforming_diagnostic_handler{
    dh, [operator_loc](diagnostic d) {
      if (operator_loc != location::unknown) {
        d.annotations.emplace_back(false, "", operator_loc);
      }
      return d;
    }};
}

struct ReadSyslogArgs {
  bool octet_counting = false;
  Option<ast::field_path> raw_message;
  multi_series_builder::options msb_options;
  location operator_location = location::unknown;
};

class ReadSyslog final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadSyslog(ReadSyslogArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await Operator<chunk_ptr, table_slice>::start(ctx);
    dh_.emplace(make_dh(ctx.dh(), args_.operator_location));
    auto raw = args_.raw_message;
    new_builder_.emplace(syslog::infuse_new_schema(args_.msb_options), *dh_,
                         raw);
    legacy_builder_.emplace(syslog::infuse_legacy_schema(args_.msb_options),
                            *dh_, raw);
    legacy_structured_builder_.emplace(
      syslog::infuse_legacy_structured_schema(args_.msb_options), *dh_, raw,
      true);
    unknown_builder_.emplace(args_.msb_options, *dh_);
    ordered_ = args_.msb_options.settings.ordered;
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_ or not input or input->size() == 0) {
      co_return;
    }
    if (args_.octet_counting) {
      TENZIR_ASSERT(dh_);
      auto result = co_await process_octet(input, push, *dh_);
      if (not result) {
        // malformed octet framing is terminal
        co_await finalize_builders(push);
        done_ = true;
        co_return;
      }
    } else {
      co_await process_lines(input, push);
    }
    co_await push_ready(push);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_await pusher_.wait();
    co_return PeriodicTick{};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(result.try_as<PeriodicTick>());
    TENZIR_UNUSED(ctx);
    if (not done_) {
      co_await push_ready(push);
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto finalize(Push<table_slice>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    TENZIR_ASSERT(dh_);
    if (args_.octet_counting) {
      if (remaining_message_length_ > 0) {
        auto const buffered_bytes = buffer_.size();
        auto const missing_bytes
          = remaining_message_length_ > buffered_bytes
              ? remaining_message_length_ - buffered_bytes
              : size_t{0};
        diagnostic::error(
          "unexpected end of input in octet-counted syslog message")
          .note("missing {} of {} bytes", missing_bytes,
                remaining_message_length_)
          .emit(*dh_);
      } else if (not buffer_.empty()) {
        diagnostic::error(
          "unexpected end of input in octet-counting length prefix")
          .emit(*dh_);
      }
    } else if (not buffer_.empty()) {
      // Flush trailing bytes as a final line in delimiter-based mode.
      co_await process_one_line(buffer_, push);
      buffer_.clear();
    }
    co_await finalize_builders(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    // Flush only committed rows. Keep pending multiline state (`last_message`)
    // in builders, and serialize it in snapshot().
    co_await flush_builders(push);
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("line_nr", line_nr_);
    serde("ended_on_cr", ended_on_carriage_return_);
    serde("remaining_msg_len", remaining_message_length_);
    serde("done", done_);
    auto last_int = static_cast<uint32_t>(last_);
    serde("last", last_int);
    last_ = static_cast<syslog::builder_tag>(last_int);

    serde("pending_new", new_builder_->last_message);
    serde("pending_new_time", new_builder_->last_message_time);
    serde("pending_legacy", legacy_builder_->last_message);
    serde("pending_legacy_time", legacy_builder_->last_message_time);
    serde("pending_legacy_structured",
          legacy_structured_builder_->last_message);
    serde("pending_legacy_structured_time",
          legacy_structured_builder_->last_message_time);
  }

private:
  struct PeriodicTick {};

  // Returns slices that must be pushed immediately when switching schema
  // (ordered mode only).
  auto flush_for_schema_change(syslog::builder_tag new_tag)
    -> std::vector<table_slice> {
    if (not ordered_ or new_tag == last_) {
      return {};
    }
    switch (last_) {
      using enum syslog::builder_tag;
      case syslog_builder:
        return new_builder_->finalize_as_table_slice();
      case legacy_syslog_builder:
        return legacy_builder_->finalize_as_table_slice();
      case legacy_structured_syslog_builder:
        return legacy_structured_builder_->finalize_as_table_slice();
      case unknown_syslog_builder:
        return unknown_builder_->finalize_as_table_slice();
    }
    TENZIR_UNREACHABLE();
  }

  auto push_ready(Push<table_slice>& push) -> Task<void> {
    auto ready = series_builder::YieldReadyResult{};
    ready.merge(new_builder_->yield_ready());
    ready.merge(legacy_builder_->yield_ready());
    ready.merge(legacy_structured_builder_->yield_ready());
    ready.merge(unknown_builder_->yield_ready());
    co_await pusher_.push(std::move(ready), push);
  }

  // Flush rows already committed to the underlying multi-series builders,
  // but keep pending multiline state (`last_message`) intact.
  auto flush_builders(Push<table_slice>& push) -> Task<void> {
    for (auto& s : new_builder_->builder.finalize_as_table_slice()) {
      co_await push(std::move(s));
    }
    for (auto& s : legacy_builder_->builder.finalize_as_table_slice()) {
      co_await push(std::move(s));
    }
    for (auto& s :
         legacy_structured_builder_->builder.finalize_as_table_slice()) {
      co_await push(std::move(s));
    }
    for (auto& s : unknown_builder_->finalize_as_table_slice()) {
      co_await push(std::move(s));
    }
  }

  // Flush all four builders, including pending multiline messages.
  auto finalize_builders(Push<table_slice>& push) -> Task<void> {
    for (auto& s : new_builder_->finalize_as_table_slice()) {
      co_await push(std::move(s));
    }
    for (auto& s : legacy_builder_->finalize_as_table_slice()) {
      co_await push(std::move(s));
    }
    for (auto& s : legacy_structured_builder_->finalize_as_table_slice()) {
      co_await push(std::move(s));
    }
    for (auto& s : unknown_builder_->finalize_as_table_slice()) {
      co_await push(std::move(s));
    }
  }

  // Parse and dispatch a single complete line.
  // Diagnostics are handled by the builders (initialised in start() with
  // the transforming_diagnostic_handler); no dh parameter is needed here.
  auto process_one_line(std::string_view line, Push<table_slice>& push)
    -> Task<void> {
    if (line.empty()) {
      co_return;
    }
    ++line_nr_;
    auto const* f = line.begin();
    auto const* const l = line.end();

    // Try RFC 5424.
    {
      auto msg = syslog::message{};
      if (syslog::message_parser{}.parse(f, l, msg)) {
        for (auto& s :
             flush_for_schema_change(syslog::builder_tag::syslog_builder)) {
          co_await push(std::move(s));
        }
        last_ = syslog::builder_tag::syslog_builder;
        if (args_.raw_message) {
          new_builder_->add_new({std::move(msg), line_nr_, std::string{line}});
        } else {
          new_builder_->add_new({std::move(msg), line_nr_});
        }
        co_return;
      }
    }

    // Try RFC 3164.
    {
      f = line.begin();
      auto legacy_msg = syslog::legacy_message{};
      if (syslog::legacy_message_parser{}.parse(f, l, legacy_msg)) {
        auto tag = syslog::get_legacy_builder_tag(legacy_msg);
        for (auto& s : flush_for_schema_change(tag)) {
          co_await push(std::move(s));
        }
        last_ = tag;
        auto& target = (tag == syslog::builder_tag::legacy_syslog_builder)
                         ? *legacy_builder_
                         : *legacy_structured_builder_;
        if (args_.raw_message) {
          target.add_new({std::move(legacy_msg), line_nr_, std::string{line}});
        } else {
          target.add_new({std::move(legacy_msg), line_nr_});
        }
        co_return;
      }
    }

    // Try Cisco legacy dialect, e.g. `<189>: 2026 Apr 14 08:45:52 UTC: ...`.
    {
      f = line.begin();
      auto cisco_msg = syslog::legacy_message{};
      if (syslog::cisco_legacy_message_parser{}.parse(f, l, cisco_msg)) {
        constexpr auto tag = syslog::builder_tag::legacy_syslog_builder;
        for (auto& s : flush_for_schema_change(tag)) {
          co_await push(std::move(s));
        }
        last_ = tag;
        if (args_.raw_message) {
          legacy_builder_->add_new(
            {std::move(cisco_msg), line_nr_, std::string{line}});
        } else {
          legacy_builder_->add_new({std::move(cisco_msg), line_nr_});
        }
        co_return;
      }
    }

    // Multiline continuation: try to append to the most recent message.
    if (last_ == syslog::builder_tag::syslog_builder
        and new_builder_->add_line_to_latest(line)) {
      co_return;
    }
    if (last_ == syslog::builder_tag::legacy_syslog_builder
        and legacy_builder_->add_line_to_latest(line)) {
      co_return;
    }
    if (last_ == syslog::builder_tag::legacy_structured_syslog_builder
        and legacy_structured_builder_->add_line_to_latest(line)) {
      co_return;
    }

    // Unknown format.
    for (auto& s :
         flush_for_schema_change(syslog::builder_tag::unknown_syslog_builder)) {
      co_await push(std::move(s));
    }
    last_ = syslog::builder_tag::unknown_syslog_builder;
    unknown_builder_->add_new({std::string{line}, line_nr_});
  }

  // Scan `input` for newlines and dispatch each complete line.
  auto process_lines(chunk_ptr const& input, Push<table_slice>& push)
    -> Task<void> {
    auto const* begin = reinterpret_cast<char const*>(input->data());
    auto const* const end = begin + input->size();

    if (ended_on_carriage_return_ and begin != end and *begin == '\n') {
      ++begin;
    }
    ended_on_carriage_return_ = false;

    for (auto const* cur = begin; cur != end; ++cur) {
      if (*cur != '\n' and *cur != '\r') {
        continue;
      }
      // Assemble line from any buffered prefix plus the bytes up to the newline.
      if (buffer_.empty()) {
        co_await process_one_line({begin, cur}, push);
      } else {
        buffer_.append(begin, cur);
        co_await process_one_line(buffer_, push);
        buffer_.clear();
      }
      if (*cur == '\r') {
        if (cur + 1 == end) {
          ended_on_carriage_return_ = true;
        } else if (*(cur + 1) == '\n') {
          ++cur;
        }
      }
      begin = cur + 1;
    }
    // Carry over any trailing bytes that haven't formed a complete line yet.
    buffer_.append(begin, end);
  }

  // Process `input` as an RFC 6587 octet-counting stream.
  //
  // `buffer_` serves a dual role depending on `remaining_message_length_`:
  //   == 0: accumulates length-prefix bytes (e.g. "57 ") until a space is found
  //   >  0: accumulates message bytes until the full count is received
  //
  // Splitting into 1-byte chunks is handled correctly because both the prefix
  // and the message are buffered across chunk boundaries.
  auto process_octet(chunk_ptr const& input, Push<table_slice>& push,
                     diagnostic_handler& dh) -> Task<failure_or<void>> {
    buffer_.append(reinterpret_cast<char const*>(input->data()), input->size());
    while (not buffer_.empty()) {
      if (remaining_message_length_ > 0) {
        // Accumulating message content — wait until we have enough bytes.
        if (buffer_.size() < remaining_message_length_) {
          break;
        }
        co_await process_one_line(
          std::string_view{buffer_.data(), remaining_message_length_}, push);
        buffer_.erase(0, remaining_message_length_);
        remaining_message_length_ = 0;
      } else {
        // Waiting for a complete length prefix.  The prefix ends with a space;
        // if no space is present yet we need more data.
        auto space_pos = buffer_.find(' ');
        if (space_pos == std::string::npos) {
          // Reject malformed prefixes eagerly: if any byte before the delimiter
          // is non-digit, this can never become a valid RFC 6587 length field.
          if (std::ranges::any_of(buffer_, [](unsigned char c) {
                return not std::isdigit(c);
              })) {
            diagnostic::error("failed to parse octet-counting length prefix")
              .emit(dh);
            buffer_.clear();
            co_return failure::promise();
          }
          // Guard against unbounded growth when the delimiter never arrives.
          // RFC 6587 uses u32 here, so anything beyond 10 digits is malformed.
          constexpr auto max_prefix_bytes
            = std::numeric_limits<uint32_t>::digits10 + 1;
          if (buffer_.size() > max_prefix_bytes) {
            diagnostic::error("octet-counting length prefix exceeds {} bytes "
                              "without delimiter",
                              max_prefix_bytes)
              .emit(dh);
            buffer_.clear();
            co_return failure::promise();
          }
          break;
        }
        // Attempt to parse "N " from the start of the buffer.
        auto it = buffer_.cbegin();
        if (not syslog::octet_length_parser(it, buffer_.cend(),
                                            remaining_message_length_)) {
          diagnostic::error("failed to parse octet-counting length prefix")
            .emit(dh);
          buffer_.clear();
          co_return failure::promise();
        }
        if (remaining_message_length_ > syslog::max_syslog_message_size) {
          diagnostic::error(
            "octet-counted message length {} exceeds maximum {}",
            remaining_message_length_, syslog::max_syslog_message_size)
            .emit(dh);
          remaining_message_length_ = 0;
          buffer_.clear();
          co_return failure::promise();
        }
        // Remove the parsed prefix bytes ("N ") from the buffer.
        buffer_.erase(0, static_cast<size_t>(it - buffer_.cbegin()));
      }
    }
    co_return {};
  }

  ReadSyslogArgs args_;

  // Initialised in start().
  Option<syslog::syslog_builder> new_builder_;
  Option<syslog::legacy_syslog_builder> legacy_builder_;
  Option<syslog::legacy_syslog_builder> legacy_structured_builder_;
  Option<syslog::unknown_syslog_builder> unknown_builder_;
  Option<transforming_diagnostic_handler> dh_;
  bool ordered_ = true;
  bool done_ = false;
  SeriesPusher pusher_;

  // Snapshotted mutable state.
  std::string buffer_;
  size_t line_nr_ = 0;
  bool ended_on_carriage_return_ = false;
  size_t remaining_message_length_ = 0;
  syslog::builder_tag last_ = syslog::builder_tag::unknown_syslog_builder;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_syslog";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadSyslogArgs, ReadSyslog>{};
    d.named("octet_counting", &ReadSyslogArgs::octet_counting);
    d.named("raw_message", &ReadSyslogArgs::raw_message);
    d.operator_location(&ReadSyslogArgs::operator_location);
    d.validate(add_msb_to_describer(d, &ReadSyslogArgs::msb_options));
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_syslog

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_syslog::plugin)
