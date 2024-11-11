//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/authenticator.hpp"

#include <tenzir/detail/base64.hpp>

#include <caf/scoped_actor.hpp>
#include <openssl/err.h>
#include <openssl/rand.h>

#include <array>

namespace tenzir::plugins::web {

caf::error authenticator_state::initialize_from(chunk_ptr chunk) {
  // We always verify here since most fields are required, so we
  // can just assert there presence below.
  auto fb = flatbuffer<fbs::ServerState, fbs::ServerStateIdentifier>::make(
    std::move(chunk));
  if (!fb)
    return fb.error();
  if ((*fb)->server_state_type() != fbs::server_state::ServerState::v0)
    return caf::make_error(ec::format_error, "unknown state version");
  auto const* state = (*fb)->server_state_as_v0();
  if (state == nullptr)
    return caf::make_error(ec::format_error, "missing state");
  TENZIR_ASSERT(state->auth_tokens() != nullptr);
  using clock = std::chrono::system_clock;
  for (auto const* token : *state->auth_tokens()) {
    TENZIR_ASSERT(token);
    auto token_desc = token_description{
      .name = token->name()->str(),
      .issued_at = clock::from_time_t(token->issued_at()),
      .expires_at = clock::from_time_t(token->expires_at()),
      .token = token->token()->str(),
    };
    tokens_.emplace_back(std::move(token_desc));
  }
  return {};
}

caf::expected<chunk_ptr> authenticator_state::save() {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<
    flatbuffers::Offset<fbs::server_state::AuthenticationTokenDescription>>
    offsets;
  for (auto& description : tokens_) {
    auto name_offset = builder.CreateString(description.name);
    auto token_offset = builder.CreateString(description.token);
    fbs::server_state::AuthenticationTokenDescriptionBuilder token_builder(
      builder);
    token_builder.add_name(name_offset);
    token_builder.add_token(token_offset);
    token_builder.add_issued_at(
      std::chrono::system_clock::to_time_t(description.issued_at));
    token_builder.add_expires_at(
      std::chrono::system_clock::to_time_t(description.expires_at));
    offsets.push_back(token_builder.Finish());
  }
  auto vector_offset = builder.CreateVector(offsets);
  fbs::server_state::v0Builder v0_builder(builder);
  v0_builder.add_auth_tokens(vector_offset);
  auto v0_offset = v0_builder.Finish();
  fbs::ServerStateBuilder state_builder(builder);
  state_builder.add_server_state_type(fbs::server_state::ServerState::v0);
  state_builder.add_server_state(v0_offset.Union());
  auto state_offset = state_builder.Finish();
  fbs::FinishServerStateBuffer(builder, state_offset);
  return chunk::make(builder.Release());
}

auto authenticator_state::generate() -> caf::expected<token_t> {
  auto random_bytes = std::array<char, 16>{};
  // Use `getrandom()` for cryptographically secure random bytes.
  auto error = RAND_bytes(reinterpret_cast<unsigned char*>(random_bytes.data()),
                          random_bytes.size());
  if (error != 1) {
    auto code = ERR_get_error();
    return caf::make_error(ec::system_error, fmt::format("could not get random "
                                                         "bytes: error {}",
                                                         code));
  }
  auto token = detail::base64::encode(
    std::string_view{random_bytes.data(), random_bytes.size()});
  // TODO: Allow the caller to pass the desired name and expiry date.
  const auto validity = std::chrono::years{10};
  auto issued_at = std::chrono::system_clock::now();
  auto expires_at = issued_at + validity;
  tokens_.push_back(token_description{
    .name = "",
    .issued_at = issued_at,
    .expires_at = expires_at,
    .token = token,
  });
  return token;
}

bool authenticator_state::authenticate(token_t x) const {
  auto now = std::chrono::system_clock::now();
  return std::find_if(tokens_.begin(), tokens_.end(),
                      [&](const token_description& description) {
                        return description.token == x
                               && now < description.expires_at;
                      })
         != tokens_.end();
}

caf::expected<authenticator_actor>
get_authenticator(caf::scoped_actor& self, node_actor node,
                  caf::timespan timeout) {
  auto maybe_authenticator = caf::expected<caf::actor>{caf::error{}};
  self
    ->request(node, timeout, atom::get_v, atom::label_v,
              std::vector<std::string>{"web"})
    .receive(
      [&](std::vector<caf::actor>& actors) {
        if (actors.empty()) {
          maybe_authenticator
            = caf::make_error(ec::logic_error,
                              "authenticator is not in component "
                              "registry; the server process may be "
                              "running without the web plugin");
        } else {
          // There should always only be one AUTHENTICATOR at a given time.
          TENZIR_ASSERT(actors.size() == 1);
          maybe_authenticator = std::move(actors[0]);
        }
      },
      [&](caf::error& err) {
        maybe_authenticator = std::move(err);
      });
  if (!maybe_authenticator)
    return maybe_authenticator.error();
  auto authenticator
    = caf::actor_cast<authenticator_actor>(std::move(*maybe_authenticator));
  return authenticator;
}

authenticator_actor::behavior_type
authenticator(authenticator_actor::stateful_pointer<authenticator_state> self,
              filesystem_actor fs) {
  self->state.path_ = std::filesystem::path{"plugins/web/authenticator.svs"};
  self->state.filesystem_ = fs;
  self->request(fs, caf::infinite, atom::read_v, self->state.path_)
    .await(
      [self](chunk_ptr chunk) {
        auto error = self->state.initialize_from(std::move(chunk));
        if (error) {
          TENZIR_ERROR("{} encountered error while deserializing state: {}",
                       *self, error);
          self->quit(error);
        }
      },
      [self](const caf::error& e) {
        if (tenzir::ec{e.code()} == tenzir::ec::no_such_file) {
          TENZIR_VERBOSE("{} starts from a fresh state", *self);
          return;
        }
        TENZIR_WARN("{} failed to load server state: {}", *self, e);
      });
  return {
    [self](atom::generate) -> caf::result<token_t> {
      auto result = self->state.generate();
      // We don't expect token generation to be very frequent and the total
      // number of tokens to be relatively small, so it should be fine to
      // re-write the complete file every time.
      auto state = self->state.save();
      if (not state) {
        return caf::make_error(ec::serialization_error,
                               fmt::format("{} failed to serialize state: {}",
                                           *self, state.error()));
      }
      auto rp = self->make_response_promise<token_t>();
      self
        ->request(self->state.filesystem_, caf::infinite, atom::write_v,
                  self->state.path_, std::move(*state))
        .then(
          [rp, result = std::move(result)](atom::ok) mutable {
            // We deliberately delay delivering the generated token until it is
            // persisted successfully, as we otherwise cannot error in case
            // persisting it fails.
            rp.deliver(std::move(result));
          },
          [rp, self](const caf::error& err) mutable {
            rp.deliver(caf::make_error(
              ec::filesystem_error,
              fmt::format("{} failed to persist token: {}", *self, err)));
          });
      return rp;
    },
    [self](atom::validate, const token_t& token) -> bool {
      return self->state.authenticate(token);
    },
    [](atom::ping) -> caf::result<void> {
      return {};
    },
  };
}

} // namespace tenzir::plugins::web
