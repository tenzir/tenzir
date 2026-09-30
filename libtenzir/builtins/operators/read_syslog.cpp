//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/pusher.hpp>
#include <tenzir/box.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/detail/syslog.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/nova/event_builder.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/read_detection.hpp>
#include <tenzir/tql2/ast.hpp>

#include <folly/coro/BoundedQueue.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <concepts>
#include <limits>
#include <span>
#include <string>
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
    if (done_) {
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

class ReadSyslogEvents final : public Operator<chunk_ptr, nova::Events> {
public:
  explicit ReadSyslogEvents(ReadSyslogArgs args)
    : args_{std::move(args)}, wait_for_{std::in_place, 1u} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    dh_.emplace(make_dh(ctx.dh(), args_.operator_location));
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await sleep_for(co_await wait_for_->dequeue());
    co_return {};
  }

  auto process_task(Any, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    co_await emit_expired(push);
  }

  auto process(chunk_ptr input, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    if (done_ or not input or input->size() == 0) {
      co_return;
    }
    auto bytes = std::string_view{reinterpret_cast<char const*>(input->data()),
                                  input->size()};
    if (args_.octet_counting) {
      co_await process_octet(bytes, push);
    } else {
      co_await process_lines(bytes, push);
    }
    if (not done_) {
      co_await emit_expired(push);
    }
  }

  auto finalize(Push<nova::Events>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    if (done_) {
      co_return FinalizeBehavior::done;
    }
    if (args_.octet_counting) {
      co_await finish_and_flush(push);
      if (remaining_message_length_ > 0) {
        diagnostic::error(
          "unexpected end of input in octet-counted syslog message")
          .note("missing {} of {} bytes",
                remaining_message_length_ - buffer_.size(),
                remaining_message_length_)
          .emit(*dh_);
        co_return FinalizeBehavior::done;
      }
      if (not buffer_.empty()) {
        diagnostic::error(
          "unexpected end of input in octet-counting length prefix")
          .emit(*dh_);
        co_return FinalizeBehavior::done;
      }
    } else if (not buffer_.empty()) {
      co_await process_one_line(buffer_, push);
      buffer_.clear();
    }
    finish_pending();
    co_await flush(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    // Only unfinished frames and the pending multiline message remain.
    // Both are bounded by the maximum message size.
    co_await flush(push);
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("ended_on_cr", ended_on_carriage_return_);
    serde("remaining_msg_len", remaining_message_length_);
    serde("done", done_);
    auto last = static_cast<uint32_t>(last_);
    serde("last", last);
    last_ = static_cast<syslog::builder_tag>(last);
    serde("pending", pending_);
    serde("pending_raw", pending_raw_);
    if (serde.is_loading()) {
      builder_ = None{};
      batch_started_ = None{};
      // Steady-clock deadlines are not portable; give restored messages
      // a fresh timeout without committing their multiline state.
      pending_started_ = pending_ ? Option{Clock::now()} : None{};
      schedule_timeout();
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  using Clock = std::chrono::steady_clock;
  using Pending = variant<syslog::message, syslog::legacy_message>;

  auto ensure_builder() -> bool {
    if (builder_) {
      return true;
    }
    auto options = args_.msb_options;
    switch (last_) {
      using enum syslog::builder_tag;
      case syslog_builder:
        options = syslog::infuse_new_schema(std::move(options));
        break;
      case legacy_syslog_builder:
        options = syslog::infuse_legacy_schema(std::move(options));
        break;
      case legacy_structured_syslog_builder:
        options = syslog::infuse_legacy_structured_schema(std::move(options));
        break;
      case unknown_syslog_builder:
        break;
    }
    auto settings = nova::event_builder_settings(options);
    settings.infer_unparsed_under = "structured_data";
    auto builder = nova::EventBuilder::make(std::move(settings), *dh_);
    if (not builder) {
      done_ = true;
      return false;
    }
    builder_ = std::move(builder).unwrap();
    return true;
  }

  auto rows() const -> size_t {
    return builder_ ? static_cast<size_t>(builder_->length()) : 0;
  }

  auto event() -> nova::EventBuilder::Record {
    if (rows() == 0) {
      batch_started_ = Clock::now();
    }
    return builder_->event();
  }

  template <class T>
  static auto add_optional(nova::EventBuilder::Record& record,
                           std::string_view name, Option<T> const& value)
    -> void {
    auto field = record.exact_field(name);
    if (not value) {
      field.null();
    } else if constexpr (std::same_as<T, uint16_t>) {
      field.data(uint64_t{*value});
    } else {
      field.data(*value);
    }
  }

  static auto add_structured_data(
    nova::EventBuilder::Record& record,
    std::vector<syslog::structured_data_element> const& elements) -> void {
    auto structured = record.exact_field("structured_data").record();
    for (auto const& element : elements) {
      auto fields = structured.field(element.id).record();
      for (auto const& [key, value] : element.params) {
        fields.field(key).data_unparsed(value);
      }
    }
  }

  auto add_raw(nova::EventBuilder::Record record,
               std::span<ast::field_path::segment const> path) -> void {
    TENZIR_ASSERT(not path.empty());
    if (path.size() == 1) {
      record.exact_field(path.front().id.name).data(pending_raw_);
      return;
    }
    add_raw(record.exact_field(path.front().id.name).record(), path.subspan(1));
  }

  auto finish_pending() -> void {
    if (not pending_ or not ensure_builder()) {
      return;
    }
    auto record = event();
    match(
      *pending_,
      [&](syslog::message& message) {
        record.exact_field("facility").data(uint64_t{message.hdr.facility});
        record.exact_field("severity").data(uint64_t{message.hdr.severity});
        record.exact_field("version").data(uint64_t{message.hdr.version});
        add_optional(record, "timestamp", message.hdr.ts);
        add_optional(record, "hostname", message.hdr.hostname);
        add_optional(record, "app_name", message.hdr.app_name);
        add_optional(record, "process_id", message.hdr.process_id);
        add_optional(record, "message_id", message.hdr.msg_id);
        syslog::merge_duplicate_sd_ids(message.data);
        add_structured_data(record, message.data);
        add_optional(record, "message", message.msg);
      },
      [&](syslog::legacy_message& message) {
        add_optional(record, "facility", message.facility);
        add_optional(record, "severity", message.severity);
        record.exact_field("timestamp").data(message.timestamp);
        add_optional(record, "hostname", message.host);
        add_optional(record, "app_name", message.tag);
        add_optional(record, "process_id", message.process_id);
        if (last_ == syslog::builder_tag::legacy_structured_syslog_builder) {
          syslog::merge_duplicate_sd_ids(message.data);
          add_structured_data(record, message.data);
        }
        record.exact_field("content").data(message.content);
      });
    if (args_.raw_message) {
      add_raw(record, args_.raw_message->path());
    }
    pending_ = None{};
    pending_raw_.clear();
    pending_started_ = None{};
  }

  auto append_continuation(std::string_view line) -> bool {
    if (not pending_) {
      return false;
    }
    auto append = [&](auto& content) {
      auto size = content.size();
      if (args_.raw_message) {
        size = std::max(size, pending_raw_.size());
      }
      if (line.size() + 1 > syslog::max_syslog_message_size - size) {
        return false;
      }
      content.push_back('\n');
      content.append(line);
      return true;
    };
    auto added = match(
      *pending_,
      [&](syslog::message& message) {
        if (message.msg) {
          return append(*message.msg);
        }
        if (args_.raw_message
            and line.size() + 1
                  > syslog::max_syslog_message_size - pending_raw_.size()) {
          return false;
        }
        message.msg.emplace(line);
        return true;
      },
      [&](syslog::legacy_message& message) {
        return append(message.content);
      });
    if (added and args_.raw_message) {
      pending_raw_.push_back('\n');
      pending_raw_.append(line);
    }
    return added;
  }

  auto change_schema(syslog::builder_tag tag, Push<nova::Events>& push)
    -> Task<void> {
    finish_pending();
    if (tag != last_) {
      co_await flush(push);
      builder_ = None{};
      last_ = tag;
    } else if (rows() >= args_.msb_options.settings.desired_batch_size) {
      co_await flush(push);
    }
  }

  auto process_one_line(std::string_view line, Push<nova::Events>& push)
    -> Task<void> {
    if (line.empty() or done_) {
      co_return;
    }
    if (line.size() > syslog::max_syslog_message_size) {
      co_await finish_and_flush(push);
      diagnostic::error("syslog message exceeds maximum {} bytes",
                        syslog::max_syslog_message_size)
        .emit(*dh_);
      done_ = true;
      co_return;
    }
    auto const* first = line.begin();
    auto const* const end = line.end();
    auto message = syslog::message{};
    auto legacy = syslog::legacy_message{};
    auto parsed = Option<Pending>{};
    auto tag = syslog::builder_tag::syslog_builder;
    if (syslog::message_parser{}.parse(first, end, message)) {
      parsed = Pending{std::move(message)};
    } else {
      first = line.begin();
      if (syslog::legacy_message_parser{}.parse(first, end, legacy)) {
        tag = syslog::get_legacy_builder_tag(legacy);
        parsed = Pending{std::move(legacy)};
      } else {
        first = line.begin();
        if (syslog::cisco_legacy_message_parser{}.parse(first, end, legacy)) {
          tag = syslog::builder_tag::legacy_syslog_builder;
          parsed = Pending{std::move(legacy)};
        }
      }
    }
    if (parsed) {
      co_await change_schema(tag, push);
      if (done_) {
        co_return;
      }
      pending_ = std::move(parsed);
      pending_started_ = Clock::now();
      if (args_.raw_message) {
        pending_raw_.assign(line);
      }
      co_return;
    }
    if (append_continuation(line)) {
      co_return;
    }
    co_await change_schema(syslog::builder_tag::unknown_syslog_builder, push);
    if (not ensure_builder()) {
      co_return;
    }
    event().exact_field("syslog_message").data(line);
    if (rows() >= args_.msb_options.settings.desired_batch_size) {
      co_await flush(push);
    }
  }

  auto append_buffer(std::string_view bytes, Push<nova::Events>& push)
    -> Task<bool> {
    if (bytes.size() > syslog::max_syslog_message_size - buffer_.size()) {
      co_await finish_and_flush(push);
      diagnostic::error("syslog message exceeds maximum {} bytes",
                        syslog::max_syslog_message_size)
        .emit(*dh_);
      done_ = true;
      co_return false;
    }
    buffer_.append(bytes);
    co_return true;
  }

  auto process_lines(std::string_view bytes, Push<nova::Events>& push)
    -> Task<void> {
    auto const* begin = bytes.begin();
    auto const* const end = bytes.end();
    if (ended_on_carriage_return_ and *begin == '\n') {
      ++begin;
    }
    ended_on_carriage_return_ = false;
    for (auto const* current = begin; current != end; ++current) {
      if (*current != '\n' and *current != '\r') {
        continue;
      }
      if (buffer_.empty()) {
        co_await process_one_line({begin, current}, push);
      } else {
        if (not co_await append_buffer({begin, current}, push)) {
          co_return;
        }
        co_await process_one_line(buffer_, push);
        buffer_.clear();
      }
      if (done_) {
        co_return;
      }
      if (*current == '\r') {
        if (current + 1 == end) {
          ended_on_carriage_return_ = true;
        } else if (*(current + 1) == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    std::ignore = co_await append_buffer({begin, end}, push);
  }

  auto process_octet(std::string_view bytes, Push<nova::Events>& push)
    -> Task<void> {
    constexpr auto max_prefix_bytes
      = std::numeric_limits<uint32_t>::digits10 + 1;
    while (not bytes.empty()) {
      if (remaining_message_length_ > 0) {
        auto count
          = std::min(bytes.size(), remaining_message_length_ - buffer_.size());
        buffer_.append(bytes.substr(0, count));
        bytes.remove_prefix(count);
        if (buffer_.size() == remaining_message_length_) {
          co_await process_one_line(buffer_, push);
          buffer_.clear();
          remaining_message_length_ = 0;
          if (done_) {
            co_return;
          }
        }
        continue;
      }
      auto byte = bytes.front();
      bytes.remove_prefix(1);
      if (byte == ' ') {
        buffer_.push_back(byte);
        auto first = buffer_.cbegin();
        if (not syslog::octet_length_parser(first, buffer_.cend(),
                                            remaining_message_length_)) {
          co_await finish_and_flush(push);
          diagnostic::error("failed to parse octet-counting length prefix")
            .emit(*dh_);
          done_ = true;
          co_return;
        }
        buffer_.clear();
        if (remaining_message_length_ > syslog::max_syslog_message_size) {
          co_await finish_and_flush(push);
          diagnostic::error(
            "octet-counted message length {} exceeds maximum {}",
            remaining_message_length_, syslog::max_syslog_message_size)
            .emit(*dh_);
          done_ = true;
          co_return;
        }
      } else if (byte < '0' or byte > '9') {
        co_await finish_and_flush(push);
        diagnostic::error("failed to parse octet-counting length prefix")
          .emit(*dh_);
        done_ = true;
        co_return;
      } else if (buffer_.size() == max_prefix_bytes) {
        co_await finish_and_flush(push);
        diagnostic::error("octet-counting length prefix exceeds {} bytes "
                          "without delimiter",
                          max_prefix_bytes)
          .emit(*dh_);
        done_ = true;
        co_return;
      } else {
        buffer_.push_back(byte);
      }
    }
  }

  auto finish_and_flush(Push<nova::Events>& push) -> Task<void> {
    // Error diagnostics cancel the pipeline, so publish accepted frames first.
    finish_pending();
    co_await flush(push);
  }

  auto flush(Push<nova::Events>& push) -> Task<void> {
    if (rows() > 0) {
      auto events = builder_->finish();
      batch_started_ = None{};
      co_await push(std::move(events));
    }
  }

  auto expired(Clock::time_point started, Clock::time_point now) const -> bool {
    auto timeout = args_.msb_options.settings.timeout;
    return timeout != duration::max() and now - started >= timeout;
  }

  auto emit_expired(Push<nova::Events>& push) -> Task<void> {
    if (done_) {
      co_return;
    }
    auto now = Clock::now();
    auto pending_expired = pending_started_ and expired(*pending_started_, now);
    if (pending_expired) {
      finish_pending();
    }
    if (pending_expired or (batch_started_ and expired(*batch_started_, now))) {
      co_await flush(push);
    }
    schedule_timeout();
  }

  auto schedule_timeout() const -> void {
    auto timeout = args_.msb_options.settings.timeout;
    if (timeout == duration::max() or done_) {
      return;
    }
    auto wait = Option<duration>{};
    auto now = Clock::now();
    for (auto const& started : {batch_started_, pending_started_}) {
      if (not started) {
        continue;
      }
      auto elapsed = now - *started;
      auto remaining
        = elapsed >= timeout ? duration::zero() : timeout - elapsed;
      wait = wait ? std::min(*wait, remaining) : remaining;
    }
    wait_for_->try_dequeue();
    if (wait) {
      wait_for_->try_enqueue(*wait);
    }
  }

  ReadSyslogArgs args_;
  Option<transforming_diagnostic_handler> dh_;
  Option<nova::EventBuilder> builder_;
  mutable Box<folly::coro::BoundedQueue<duration>> wait_for_;
  Option<Clock::time_point> batch_started_;
  Option<Clock::time_point> pending_started_;
  Option<Pending> pending_;
  std::string pending_raw_;
  std::string buffer_;
  size_t remaining_message_length_ = 0;
  syslog::builder_tag last_ = syslog::builder_tag::unknown_syslog_builder;
  bool ended_on_carriage_return_ = false;
  bool done_ = false;
};

class plugin final : public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_syslog";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadSyslogArgs, ReadSyslog, ReadSyslogEvents>{};
    d.named("octet_counting", &ReadSyslogArgs::octet_counting);
    auto raw = d.named("raw_message", &ReadSyslogArgs::raw_message);
    d.operator_location(&ReadSyslogArgs::operator_location);
    auto msb = add_msb_to_describer(d, &ReadSyslogArgs::msb_options);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      msb(ctx);
      nova::validate_event_builder_options(msb, ctx);
      if (auto field = ctx.get(raw); field and field->path().empty()) {
        diagnostic::error("`raw_message` must specify a field")
          .primary(field->get_location())
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate(
        "read_syslog", read_detection::specificity::grammar, detect_syslog),
    };
  }

private:
  static auto detect_syslog(read_detection_input input)
    -> read_detection_result {
    namespace rd = read_detection;
    auto sample = rd::sample_lines(input, 2);
    std::erase_if(sample.complete, [](std::string_view& line) {
      line = detail::trim_front(line);
      return line.empty();
    });
    if (sample.complete.empty()) {
      auto partial = detail::trim_front(sample.partial);
      if (partial.empty()) {
        return input.eof ? rd::reject() : rd::need_more();
      }
      if (not partial.starts_with('<')) {
        return rd::reject();
      }
      return rd::need_more();
    }
    auto line = sample.complete.front();
    // RFC 3164 messages without a PRI prefix are indistinguishable from
    // free-form text that happens to start with a timestamp; require the
    // prefix for automatic detection and dry-run the actual parsers.
    if (not line.starts_with('<')) {
      return rd::reject();
    }
    auto const* f = line.begin();
    auto const* const l = line.end();
    if (syslog::message_parser{}.parse(f, l, unused)) {
      return rd::match();
    }
    f = line.begin();
    if (syslog::legacy_message_parser{}.parse(f, l, unused)) {
      return rd::match();
    }
    f = line.begin();
    // The Cisco parser does not support parsing into `unused`.
    auto cisco_msg = syslog::legacy_message{};
    if (syslog::cisco_legacy_message_parser{}.parse(f, l, cisco_msg)) {
      return rd::match();
    }
    return rd::reject();
  }
};

} // namespace

} // namespace tenzir::plugins::read_syslog

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_syslog::plugin)
