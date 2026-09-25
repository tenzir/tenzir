//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/pusher.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>

#include <arrow/util/utf8.h>

#include <string_view>

namespace tenzir::plugins::read_delimited {

namespace {

struct ReadDelimitedArgs {
  located<data> separator;
  Option<bool> binary;
  bool include_separator = false;
};

class ReadDelimited final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadDelimited(ReadDelimitedArgs args) : args_{std::move(args)} {
    match(
      args_.separator.inner,
      [&](const std::string& s) {
        separator_ = s;
      },
      [&](const blob& b) {
        separator_.assign(reinterpret_cast<const char*>(b.data()), b.size());
      },
      [](const auto&) {
        TENZIR_UNREACHABLE();
      });
    // Auto-resolve binary mode: true for blob separators, false for string.
    binary_ = args_.binary.unwrap_or(is<blob>(args_.separator.inner));
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    buffer_.append(reinterpret_cast<const char*>(input->data()), input->size());
    auto remaining = std::string_view{buffer_};
    while (true) {
      const auto pos = remaining.find(separator_);
      if (pos == std::string::npos) {
        break;
      }
      const auto end = args_.include_separator ? pos + separator_.size() : pos;
      const auto seg = remaining.substr(0, end);
      emit(seg, ctx);
      remaining = remaining.substr(pos + separator_.size());
    }
    buffer_ = buffer_.substr(buffer_.size() - remaining.size());
    co_await pusher_.push(builder_.yield_ready(type_name), push);
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    co_await pusher_.push(builder_.yield_ready(type_name), push);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    // `buffer_` holds only data after the last separator (i.e., no complete
    // separator remains in it); emit it as a final partial record if non-empty.
    if (not buffer_.empty()) {
      emit(buffer_, ctx);
    }
    buffer_.clear();
    co_await flush(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    co_await flush(push);
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
  }

private:
  auto flush(Push<table_slice>& push) -> Task<void> {
    if (builder_.length() > 0) {
      co_await push(builder_.finish_assert_one_slice("tenzir.data"));
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

  constexpr static const auto type_name = "tenzir.data";

  ReadDelimitedArgs args_;
  std::string separator_;
  bool binary_ = false;
  std::string buffer_;
  series_builder builder_;
  SeriesPusher pusher_;
};

class ReadDelimitedEvents final : public Operator<chunk_ptr, nova::Events> {
public:
  explicit ReadDelimitedEvents(ReadDelimitedArgs args)
    : args_{std::move(args)} {
    match(
      args_.separator.inner,
      [&](std::string const& s) {
        separator_ = s;
      },
      [&](blob const& b) {
        separator_.assign(reinterpret_cast<char const*>(b.data()), b.size());
      },
      [](auto const&) {
        TENZIR_UNREACHABLE();
      });
    binary_ = args_.binary.unwrap_or(is<blob>(args_.separator.inner));
  }

  auto process(chunk_ptr input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    buffer_.append(reinterpret_cast<char const*>(input->data()), input->size());
    auto remaining = std::string_view{buffer_};
    while (true) {
      auto const pos = remaining.find(separator_);
      if (pos == std::string::npos) {
        break;
      }
      auto const end = args_.include_separator ? pos + separator_.size() : pos;
      emit(remaining.substr(0, end), ctx);
      remaining = remaining.substr(pos + separator_.size());
      if (static_cast<uint64_t>(builder_.length())
          >= defaults::import::table_slice_size) {
        co_await flush(push);
      }
    }
    buffer_.erase(0, buffer_.size() - remaining.size());
    if (timeout_.poll(builder_.length())) {
      co_await flush(push);
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await timeout_.wait();
    co_return {};
  }

  auto process_task(Any, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    if (timeout_.poll(builder_.length())) {
      co_await flush(push);
    }
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    // `buffer_` holds only data after the last separator; emit it as a final
    // partial record if non-empty.
    if (not buffer_.empty()) {
      emit(buffer_, ctx);
    }
    buffer_.clear();
    co_await flush(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    co_await flush(push);
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
  }

private:
  auto flush(Push<nova::Events>& push) -> Task<void> {
    if (builder_.length() == 0) {
      co_return;
    }
    auto data = builder_.finish();
    builder_ = nova::ArrayBuilder<nova::Record>{};
    timeout_.reset();
    auto const length = data.length();
    co_await push(
      nova::Events{std::move(data), nova::storage::BitMap{length, true},
                   nova::Events::Meta::make_empty(length, type_name)});
  }

  auto emit(std::string_view segment, OpCtx& ctx) -> void {
    if (binary_) {
      builder_.record().field("data").data(blob_view{as_bytes(segment)});
      return;
    }
    if (not arrow::util::ValidateUTF8(segment)) {
      diagnostic::warning("got invalid UTF-8")
        .hint("use `binary=true` if you are reading binary data")
        .emit(ctx);
      return;
    }
    builder_.record().field("data").data(segment);
  }

  constexpr static auto type_name = "tenzir.data";

  ReadDelimitedArgs args_;
  std::string separator_;
  bool binary_ = false;
  std::string buffer_;
  nova::ArrayBuilder<nova::Record> builder_;
  BatchTimeout timeout_{defaults::import::batch_timeout};
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_delimited";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadDelimitedArgs, ReadDelimited, ReadDelimitedEvents>{};
    auto sep
      = d.positional("separator", &ReadDelimitedArgs::separator, "string|blob");
    d.named("binary", &ReadDelimitedArgs::binary);
    d.named("include_separator", &ReadDelimitedArgs::include_separator);
    d.validate([sep](DescribeCtx& ctx) -> Empty {
      TRY(auto s, ctx.get(sep));
      if (not is<std::string>(s.inner) and not is<blob>(s.inner)) {
        diagnostic::error("separator must be a `string` or `blob`")
          .primary(s.source)
          .emit(ctx);
        return {};
      }
      const auto size = is<std::string>(s.inner)
                          ? as<std::string>(s.inner).size()
                          : as<blob>(s.inner).size();
      if (size == 0) {
        diagnostic::error("separator must not be empty")
          .primary(s.source)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_delimited

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_delimited::plugin)
