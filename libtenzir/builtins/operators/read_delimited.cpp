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

#include <string_view>

namespace tenzir::plugins::read_delimited {

namespace {

using std::chrono::steady_clock;

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

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (flush_at_) {
      co_await sleep_until(*flush_at_);
    } else {
      co_await sleep_for(defaults::import::batch_timeout);
    }
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    if (flush_at_ && steady_clock::now() >= *flush_at_) {
      co_await flush(push);
    }
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (input->size() == 0) {
      co_return;
    }
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
    if (static_cast<uint64_t>(builder_.length())
        >= defaults::import::table_slice_size) {
      co_await flush(push);
    } else {
      flush_at_ = Option{steady_clock::now() + defaults::import::batch_timeout};
    }
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
    flush_at_ = None{};
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

  ReadDelimitedArgs args_;
  std::string separator_;
  bool binary_ = false;
  std::string buffer_;
  series_builder builder_;
  Option<steady_clock::time_point> flush_at_ = None{};
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_delimited";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadDelimitedArgs, ReadDelimited>{};
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
