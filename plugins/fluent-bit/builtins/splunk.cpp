//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>

#include "fluent-bit/fluent-bit_operator.hpp"

namespace tenzir::plugins::fluentbit {

namespace {

class splunk_plugin final : public virtual operator_parser_plugin,
                            public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {
      .sink = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    std::optional<located<std::string>> url = {};
    std::optional<located<std::string>> token = {};
    std::optional<located<std::string>> http_user = {};
    std::optional<located<std::string>> http_password = {};
    std::optional<located<uint64_t>> http_buffer_size = {};
    std::optional<location> compress = {};
    std::optional<located<std::string>> channel = {};
    std::optional<located<std::string>> event_host = {};
    std::optional<located<std::string>> event_source = {};
    std::optional<located<std::string>> event_sourcetype = {};
    std::optional<located<std::string>> event_index = {};
    std::optional<location> tls = {};
    std::optional<located<std::string>> cafile = {};
    std::optional<located<std::string>> certfile = {};
    std::optional<located<std::string>> keyfile = {};
    std::optional<located<std::string>> keyfile_password = {};
    auto parser = argument_parser{
      "splunk",
      "https://docs.tenzir.com/operators/splunk",
    };
    parser.add(url, "<url>");
    parser.add("--token", token, "<string>");
    parser.add("--http-user", http_user, "<string>");
    parser.add("--http-password", http_password, "<string>");
    parser.add("--http-buffer-size", http_buffer_size, "<uint64>");
    parser.add("--compress", compress);
    parser.add("--channel", channel, "<string>");
    parser.add("--event-host", event_host, "<string>");
    parser.add("--event-source", event_source, "<string>");
    parser.add("--event-sourcetype", event_sourcetype, "<string>");
    parser.add("--event-index", event_index, "<string>");
    parser.add("--tls", tls);
    parser.add("--cafile", cafile, "<string>");
    parser.add("--certfile", certfile, "<string>");
    parser.add("--keyfile", keyfile, "<string>");
    parser.add("--keyfile-password", keyfile_password, "<string>");
    parser.parse(p);
    auto args = operator_args{};
    args.plugin = "splunk";
    if (url) {
      auto url_view = std::string_view{url->inner};
      if (url_view.starts_with("splunk://")) {
        url_view.remove_prefix(9);
      }
      auto [host, port] = detail::split_once(url_view, ":");
      if (not host.empty()) {
        args.args["host"] = host;
      }
      if (not port.empty()) {
        args.args["port"] = port;
      }
    }
    auto try_assign = [&]<class T>(auto name, const T& arg) {
      if (arg) {
        if constexpr (std::is_same_v<T, std::optional<location>>) {
          args.args[name] = "on";
        } else if constexpr (std::is_same_v<T, located<std::string>>) {
          args.args[name] = arg->inner;
        } else {
          args.args[name] = fmt::to_string(arg->inner);
        }
      }
    };
    try_assign("token", token);
    try_assign("http_user", http_user);
    try_assign("http_passwd", http_password);
    try_assign("http_buffer_size", http_buffer_size);
    try_assign("compress", compress);
    try_assign("channel", channel);
    try_assign("event_host", event_host);
    try_assign("event_source", event_source);
    try_assign("event_sourcetype", event_sourcetype);
    try_assign("event_index", event_index);
    try_assign("tls", tls);
    try_assign("tls_ca_file", cafile);
    try_assign("tls_crt_file", certfile);
    try_assign("tls_key_file", keyfile);
    try_assign("tls_key_passwd", keyfile_password);
    args.args["splunk_send_raw"] = "on";
    return std::make_unique<fluent_bit_operator>(
      std::move(args), multi_series_builder::options{}, record{});
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    std::optional<located<std::string>> url = {};
    std::optional<located<std::string>> token = {};
    std::optional<located<std::string>> http_user = {};
    std::optional<located<std::string>> http_password = {};
    std::optional<located<uint64_t>> http_buffer_size = {};
    std::optional<location> compress = {};
    std::optional<located<std::string>> channel = {};
    std::optional<located<std::string>> event_host = {};
    std::optional<located<std::string>> event_source = {};
    std::optional<located<std::string>> event_sourcetype = {};
    std::optional<located<std::string>> event_index = {};
    std::optional<location> tls = {};
    std::optional<located<std::string>> cafile = {};
    std::optional<located<std::string>> certfile = {};
    std::optional<located<std::string>> keyfile = {};
    std::optional<located<std::string>> keyfile_password = {};
    argument_parser2::operator_("splunk")
      .add(url, "<url>")
      .add("splunk_token", token)
      .add("http_user", http_user)
      .add("http_password", http_password)
      .add("http_buffer_size", http_buffer_size)
      .add("compress", compress)
      .add("channel", channel)
      .add("event_host", event_host)
      .add("event_source", event_source)
      .add("event_sourcetype", event_sourcetype)
      .add("event_index", event_index)
      .add("tls", tls)
      .add("cafile", cafile)
      .add("certfile", certfile)
      .add("keyfile", keyfile)
      .add("keyfile_password", keyfile_password)
      .parse(inv, ctx)
      .ignore();
    auto args = operator_args{};
    args.plugin = "splunk";
    if (url) {
      auto url_view = std::string_view{url->inner};
      if (url_view.starts_with("splunk://")) {
        url_view.remove_prefix(9);
      }
      auto [host, port] = detail::split_once(url_view, ":");
      if (not host.empty()) {
        args.args["host"] = host;
      }
      if (not port.empty()) {
        args.args["port"] = port;
      }
    }
    auto try_assign = [&]<class T>(auto name, const T& arg) {
      if (arg) {
        if constexpr (std::is_same_v<T, std::optional<location>>) {
          args.args[name] = "on";
        } else if constexpr (std::is_same_v<T, located<std::string>>) {
          args.args[name] = arg->inner;
        } else {
          args.args[name] = fmt::to_string(arg->inner);
        }
      }
    };
    try_assign("splunk_token", token);
    try_assign("http_user", http_user);
    try_assign("http_passwd", http_password);
    try_assign("http_buffer_size", http_buffer_size);
    try_assign("compress", compress);
    try_assign("channel", channel);
    try_assign("event_host", event_host);
    try_assign("event_source", event_source);
    try_assign("event_sourcetype", event_sourcetype);
    try_assign("event_index", event_index);
    try_assign("tls", tls);
    try_assign("tls_ca_file", cafile);
    try_assign("tls_crt_file", certfile);
    try_assign("tls_key_file", keyfile);
    try_assign("tls_key_passwd", keyfile_password);
    args.args["splunk_send_raw"] = "on";
    return std::make_unique<fluent_bit_operator>(
      std::move(args), multi_series_builder::options{}, record{});
  }

  auto name() const -> std::string override {
    return "splunk";
  }
};

} // namespace

} // namespace tenzir::plugins::fluentbit

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fluentbit::splunk_plugin)
