//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Encryption functions for TQL:
//
// - `encrypt_aes_gcm`, `encrypt_aes_gcm_siv`, and `encrypt_aes_siv` with their
//   `decrypt_*` counterparts provide authenticated symmetric encryption.
// - `encrypt_hpke` and `decrypt_hpke` provide public-key encryption (RFC 9180).
// - `encrypt_ff1` and `decrypt_ff1` provide format-preserving encryption
//   (NIST SP 800-38G).
//
// All functions take their keys as secrets, except for the public key of
// `encrypt_hpke`, and are available with the Nova executor only.

#include <tenzir/blob.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/secret.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/result.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view.hpp>

#include <fmt/format.h>
#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hpke.h>
#include <openssl/pem.h>
#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <climits>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::encryption {

namespace {

using Bytes = std::span<std::byte const>;

/// The size of the authentication tag of every AEAD in this file.
constexpr auto tag_size = size_t{16};

// -- OpenSSL utilities --------------------------------------------------------

template <class T, auto Free>
struct OpensslDeleter {
  auto operator()(T* ptr) const noexcept -> void {
    Free(ptr);
  }
};

template <class T, auto Free>
using OpensslPtr = std::unique_ptr<T, OpensslDeleter<T, Free>>;

using CipherPtr = OpensslPtr<EVP_CIPHER, EVP_CIPHER_free>;
using CipherCtxPtr = OpensslPtr<EVP_CIPHER_CTX, EVP_CIPHER_CTX_free>;
using PkeyPtr = OpensslPtr<EVP_PKEY, EVP_PKEY_free>;
using HpkeCtxPtr = OpensslPtr<OSSL_HPKE_CTX, OSSL_HPKE_CTX_free>;
using BioPtr = OpensslPtr<BIO, BIO_free_all>;
using BnPtr = OpensslPtr<BIGNUM, BN_free>;
using BnCtxPtr = OpensslPtr<BN_CTX, BN_CTX_free>;

/// The word type of OpenSSL big numbers, which `BN_ULONG` defines as a macro.
using BnWord = BN_ULONG;

auto u8(std::byte const* ptr) -> unsigned char const* {
  return reinterpret_cast<unsigned char const*>(ptr);
}

auto u8(std::byte* ptr) -> unsigned char* {
  return reinterpret_cast<unsigned char*>(ptr);
}

/// A buffer that stands in for empty inputs and outputs. Some OpenSSL code
/// paths reject null pointers even for empty buffers, and the SIV modes only
/// compute their tag when the update receives an output buffer.
using Placeholder = std::array<unsigned char, tag_size>;

auto data_or(Bytes bytes, Placeholder const& placeholder)
  -> unsigned char const* {
  return bytes.empty() ? placeholder.data() : u8(bytes.data());
}

auto data_or(blob& bytes, Placeholder& placeholder) -> unsigned char* {
  return bytes.empty() ? placeholder.data() : u8(bytes.data());
}

/// Returns and clears the most recent OpenSSL error of this thread.
auto openssl_error() -> std::string {
  auto const code = ERR_peek_last_error();
  ERR_clear_error();
  if (code == 0) {
    return "unknown error";
  }
  auto buffer = std::array<char, 256>{};
  ERR_error_string_n(code, buffer.data(), buffer.size());
  return buffer.data();
}

auto fetch_cipher(std::string const& name) -> CipherPtr {
  return CipherPtr{EVP_CIPHER_fetch(nullptr, name.c_str(), nullptr)};
}

auto key_bytes(located<nova::Secret> const& key) -> Bytes {
  return Bytes{key.inner.data.data(), key.inner.data.size()};
}

// -- Argument handling --------------------------------------------------------

/// The view types of byte sequences that the kernels accept.
template <class T>
concept BytesView = concepts::one_of<T, std::string_view, blob_view>;

auto bytes_of(std::string_view value) -> Bytes {
  return std::as_bytes(std::span{value.data(), value.size()});
}

auto bytes_of(blob_view value) -> Bytes {
  return value;
}

/// The warning for a `null` value of an optional argument that changes the
/// result, such as associated data or a tweak. Treating `null` as empty would
/// silently weaken the binding to the context, so the result is `null`.
auto null_argument_warning(std::string_view name, location source)
  -> diagnostic_builder {
  return diagnostic::warning("`{}` is null", name)
    .primary(source)
    .note("the function returns `null` when `{}` is `null`", name)
    .hint("use `else \"\"` to fall back to an empty value");
}

/// Invokes `f(dh, x, extra, is_string)` for every row where `x` and `extra`
/// are strings or blobs. When the optional argument `extra_name` is absent,
/// `extra` is empty. `is_string` tells whether `x` is a string. Rows with a
/// `null` value of `x` become `null`. Rows with a `null` value of `extra`
/// become `null` with a warning, and other types become `null` with a warning.
template <class F>
auto apply_bytes(nova::EvalFrame frame, std::string_view name,
                 nova::ValueArgument const& x,
                 Option<nova::ValueArgument> const& extra,
                 std::string_view extra_name, location call, F const& f)
  -> nova::Array<nova::Data> {
  if (not extra) {
    return nova::apply_kernel<1>(
      frame, name, std::array<nova::ValueArgument, 1>{x}, x.source,
      [&]<BytesView X>(diagnostic_handler& dh, X value) {
        return f(dh, bytes_of(value), Bytes{},
                 std::same_as<X, std::string_view>);
      });
  }
  using Output
    = std::invoke_result_t<F const&, diagnostic_handler&, Bytes, Bytes, bool>;
  auto null_extra = nova::WarnOnce{};
  return nova::apply_kernel<2>(
    frame, name, std::array<nova::ValueArgument, 2>{x, *extra}, call,
    detail::overload{
      [&]<BytesView X, BytesView E>(diagnostic_handler& dh, X value, E other) {
        return f(dh, bytes_of(value), bytes_of(other),
                 std::same_as<X, std::string_view>);
      },
      [&]<BytesView X>(diagnostic_handler& dh, X, nova::Null) -> Output {
        null_extra(dh, null_argument_warning(extra_name, extra->source));
        return None{};
      },
    });
}

auto requires_nova(std::string_view name, function_invocation const& inv,
                   session ctx) -> failure_or<function_ptr> {
  diagnostic::error("`{}` requires `--nova`", name).primary(inv.call).emit(ctx);
  return failure::promise();
}

/// Adds a hint for string inputs to a decryption failure. A string most
/// likely holds an encoded ciphertext, such as Base64 from JSON.
auto with_encoding_hint(diagnostic_builder diag, bool is_string)
  -> diagnostic_builder {
  if (not is_string) {
    return diag;
  }
  return std::move(diag).hint("decode Base64 or hex input with "
                              "`decode_base64` or `decode_hex` first");
}

// -- AES-GCM, AES-GCM-SIV, and AES-SIV ----------------------------------------

enum class AesMode { gcm, gcm_siv, siv };

auto aes_suffix(AesMode mode) -> std::string_view {
  switch (mode) {
    case AesMode::gcm:
      return "aes_gcm";
    case AesMode::gcm_siv:
      return "aes_gcm_siv";
    case AesMode::siv:
      return "aes_siv";
  }
  TENZIR_UNREACHABLE();
}

/// The size of the random nonce that prefixes the ciphertext. AES-SIV derives
/// its IV from the input and therefore has none.
constexpr auto aes_nonce_size(AesMode mode) -> size_t {
  return mode == AesMode::siv ? 0 : 12;
}

/// The supported key sizes, for diagnostics.
auto aes_key_sizes(AesMode mode) -> std::string_view {
  switch (mode) {
    case AesMode::gcm:
      return "16, 24, or 32 bytes";
    case AesMode::gcm_siv:
      return "16 or 32 bytes";
    case AesMode::siv:
      return "32, 48, or 64 bytes";
  }
  TENZIR_UNREACHABLE();
}

/// Selects the OpenSSL cipher by key size. AES-SIV splits its key into a MAC
/// key and an encryption key, so it needs twice the AES key size. RFC 8452
/// defines AES-GCM-SIV only for 128-bit and 256-bit keys.
auto aes_cipher_name(AesMode mode, size_t key_size) -> Option<std::string> {
  auto bits = size_t{0};
  switch (mode) {
    case AesMode::gcm:
      if (key_size == 16 or key_size == 24 or key_size == 32) {
        bits = key_size * 8;
      }
      break;
    case AesMode::gcm_siv:
      if (key_size == 16 or key_size == 32) {
        bits = key_size * 8;
      }
      break;
    case AesMode::siv:
      if (key_size == 32 or key_size == 48 or key_size == 64) {
        bits = key_size * 4;
      }
      break;
  }
  if (bits == 0) {
    return None{};
  }
  auto const name = std::string_view{
    mode == AesMode::gcm       ? "GCM"
    : mode == AesMode::gcm_siv ? "GCM-SIV"
                               : "SIV",
  };
  return fmt::format("AES-{}-{}", bits, name);
}

/// Feeds the associated data into an initialized cipher context. AES-SIV
/// authenticates a vector of associated data strings. It always receives
/// exactly one, which is empty when `aad` is absent. This matches the
/// deterministic AEAD of Tink and makes an absent `aad` equivalent to an empty
/// one, like in the other modes.
template <class Update>
auto aes_aad(AesMode mode, EVP_CIPHER_CTX* ctx, Bytes aad, Update update)
  -> bool {
  if (mode != AesMode::siv and aad.empty()) {
    return true;
  }
  auto const placeholder = Placeholder{};
  auto length = 0;
  return update(ctx, nullptr, &length, data_or(aad, placeholder),
                static_cast<int>(aad.size()))
         == 1;
}

/// Encrypts `plaintext` into `nonce || ciphertext || tag`, or into
/// `tag || ciphertext` for AES-SIV, whose tag is the synthetic IV.
auto aes_seal(AesMode mode, EVP_CIPHER_CTX* ctx, EVP_CIPHER const* cipher,
              Bytes key, Bytes plaintext, Bytes aad) -> Option<blob> {
  auto const nonce_size = aes_nonce_size(mode);
  if (plaintext.size() > INT_MAX - nonce_size - tag_size
      or aad.size() > INT_MAX) {
    return None{};
  }
  auto placeholder = Placeholder{};
  auto result = blob(nonce_size + plaintext.size() + tag_size);
  auto* out = u8(result.data());
  auto* nonce = nonce_size == 0 ? nullptr : out;
  auto* ciphertext = mode == AesMode::siv ? out + tag_size : out + nonce_size;
  auto* tag = mode == AesMode::siv ? out : ciphertext + plaintext.size();
  if (nonce and RAND_bytes(nonce, static_cast<int>(nonce_size)) != 1) {
    return None{};
  }
  // Passing the cipher resets the context completely, which discards all
  // state of the previous value. Never pass `nullptr` to reuse the keyed
  // context: AES-GCM-SIV keeps its associated data across such a
  // reinitialization, so a value without associated data would inherit the
  // associated data of the previous value.
  if (EVP_EncryptInit_ex2(ctx, cipher, u8(key.data()), nonce, nullptr) != 1) {
    return None{};
  }
  if (not aes_aad(mode, ctx, aad, EVP_EncryptUpdate)) {
    return None{};
  }
  auto length = 0;
  if (EVP_EncryptUpdate(ctx, ciphertext, &length,
                        data_or(plaintext, placeholder),
                        static_cast<int>(plaintext.size()))
      != 1) {
    return None{};
  }
  auto final_length = 0;
  if (EVP_EncryptFinal_ex(ctx, ciphertext + length, &final_length) != 1) {
    return None{};
  }
  TENZIR_ASSERT(static_cast<size_t>(length + final_length) == plaintext.size());
  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_GET_TAG,
                          static_cast<int>(tag_size), tag)
      != 1) {
    return None{};
  }
  return result;
}

/// Why a decryption failed.
enum class OpenError {
  /// The decryption could not start. The OpenSSL error queue holds the
  /// reason.
  setup,
  /// The input failed authentication.
  authentication,
};

/// Decrypts the output of `aes_seal`. The caller must ensure that `input`
/// holds at least the nonce and the tag.
auto aes_open(AesMode mode, EVP_CIPHER_CTX* ctx, EVP_CIPHER const* cipher,
              Bytes key, Bytes input, Bytes aad) -> Result<blob, OpenError> {
  auto const nonce_size = aes_nonce_size(mode);
  TENZIR_ASSERT(input.size() >= nonce_size + tag_size);
  if (input.size() > INT_MAX or aad.size() > INT_MAX) {
    return Err{OpenError::setup};
  }
  auto const ciphertext_size = input.size() - nonce_size - tag_size;
  auto const* in = u8(input.data());
  auto const* nonce = nonce_size == 0 ? nullptr : in;
  auto const* ciphertext
    = mode == AesMode::siv ? in + tag_size : in + nonce_size;
  auto const* tag = mode == AesMode::siv ? in : ciphertext + ciphertext_size;
  // As in `aes_seal`, the cipher argument must reset the context.
  if (EVP_DecryptInit_ex2(ctx, cipher, u8(key.data()), nonce, nullptr) != 1) {
    return Err{OpenError::setup};
  }
  // The SIV modes use the tag as the IV of the keystream, so we must set it
  // before decrypting.
  auto expected_tag = Placeholder{};
  std::memcpy(expected_tag.data(), tag, tag_size);
  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_TAG,
                          static_cast<int>(tag_size), expected_tag.data())
      != 1) {
    return Err{OpenError::setup};
  }
  if (not aes_aad(mode, ctx, aad, EVP_DecryptUpdate)) {
    return Err{OpenError::setup};
  }
  auto placeholder = Placeholder{};
  auto result = blob(ciphertext_size);
  auto* out = data_or(result, placeholder);
  // We always call the update, even for an empty ciphertext. Before the fix
  // for CVE-2026-45446, OpenSSL computed the SIV tags only in the update and
  // accepted forged empty messages otherwise.
  auto length = 0;
  if (EVP_DecryptUpdate(ctx, out, &length, ciphertext,
                        static_cast<int>(ciphertext_size))
      != 1) {
    // AES-SIV verifies the tag in the update, the other modes in the final
    // step.
    return Err{mode == AesMode::siv ? OpenError::authentication
                                    : OpenError::setup};
  }
  auto final_length = 0;
  if (EVP_DecryptFinal_ex(ctx, out + length, &final_length) != 1) {
    return Err{OpenError::authentication};
  }
  return result;
}

/// The warning for a decryption that failed for `error`.
auto open_warning(OpenError error, std::string_view note, location source,
                  bool is_string) -> diagnostic_builder {
  if (error == OpenError::setup) {
    return diagnostic::warning("failed to decrypt: {}", openssl_error())
      .primary(source);
  }
  ERR_clear_error();
  return with_encoding_hint(diagnostic::warning("failed to decrypt: "
                                                "authentication failed")
                              .primary(source)
                              .note(std::string{note}),
                            is_string);
}

struct AesArgs {
  nova::ValueArgument x;
  located<nova::Secret> key;
  Option<nova::ValueArgument> aad;
  location call;
  /// The OpenSSL cipher, selected by key size in `validate`.
  std::string cipher;
};

template <AesMode Mode, bool Encrypt>
auto aes_function_name() -> std::string {
  return fmt::format("{}_{}", Encrypt ? "encrypt" : "decrypt",
                     aes_suffix(Mode));
}

template <AesMode Mode, bool Encrypt>
class AesFunction {
public:
  static auto eval(AesArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto const name = aes_function_name<Mode, Encrypt>();
    auto cipher = fetch_cipher(args.cipher);
    auto ctx = CipherCtxPtr{EVP_CIPHER_CTX_new()};
    if (not cipher or not ctx) {
      diagnostic::warning("failed to initialize `{}`: {}", name,
                          openssl_error())
        .primary(args.call)
        .emit(frame);
      return frame.null();
    }
    auto const key = key_bytes(args.key);
    auto too_short = nova::WarnOnce{};
    auto failed = nova::WarnOnce{};
    auto failed_setup = nova::WarnOnce{};
    return apply_bytes(
      frame, name, args.x, args.aad, "aad", args.call,
      [&](diagnostic_handler& dh, Bytes input, Bytes aad,
          [[maybe_unused]] bool is_string) -> Option<blob> {
        if constexpr (Encrypt) {
          auto result
            = aes_seal(Mode, ctx.get(), cipher.get(), key, input, aad);
          if (not result) {
            failed(dh,
                   diagnostic::warning("failed to encrypt: {}", openssl_error())
                     .primary(args.x.source));
          }
          return result;
        } else {
          auto const min_size = aes_nonce_size(Mode) + tag_size;
          if (input.size() < min_size) {
            too_short(dh, with_encoding_hint(
                            diagnostic::warning("ciphertext is too short")
                              .primary(args.x.source)
                              .note("expected at least {} bytes", min_size),
                            is_string));
            return None{};
          }
          auto result
            = aes_open(Mode, ctx.get(), cipher.get(), key, input, aad);
          if (result.is_err()) {
            auto const error = result.unwrap_err();
            auto& latch = error == OpenError::setup ? failed_setup : failed;
            latch(dh, open_warning(error,
                                   "the key or the associated data does not "
                                   "match, or the ciphertext was modified",
                                   args.x.source, is_string));
            return None{};
          }
          return std::move(result).unwrap();
        }
      });
  }
};

template <AesMode Mode, bool Encrypt>
class AesPlugin final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return aes_function_name<Mode, Encrypt>();
  }

  auto is_deterministic() const -> bool override {
    // Randomized encryption draws a fresh nonce for every value.
    return not Encrypt or Mode == AesMode::siv;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<AesArgs, AesFunction<Mode, Encrypt>>{};
    d.positional("x", &AesArgs::x, "blob|string");
    d.named("key", &AesArgs::key);
    d.named_optional("aad", &AesArgs::aad, "blob|string");
    d.call_location(&AesArgs::call);
    d.validate([](AesArgs& args, diagnostic_handler& dh) -> failure_or<void> {
      auto const size = args.key.inner.data.size();
      auto cipher = aes_cipher_name(Mode, size);
      if (not cipher) {
        diagnostic::error("`key` must have {}, but has {} bytes",
                          aes_key_sizes(Mode), size)
          .primary(args.key.source)
          .hint("decode a hex or Base64 key with `decode_hex` or "
                "`decode_base64`, for example "
                "`secret(\"key\").decode_hex()`")
          .emit(dh);
        return failure::promise();
      }
      if (not fetch_cipher(*cipher)) {
        diagnostic::error("cipher `{}` is not available: {}", *cipher,
                          openssl_error())
          .primary(args.call)
          .emit(dh);
        return failure::promise();
      }
      args.cipher = std::move(*cipher);
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    return requires_nova(name(), inv, ctx);
  }
};

// -- HPKE ---------------------------------------------------------------------

/// Rejects encrypted PEM files instead of prompting for a passphrase.
auto no_passphrase(char*, int, int, void*) -> int {
  return 0;
}

auto as_text(Bytes bytes) -> std::string_view {
  return {reinterpret_cast<char const*>(bytes.data()), bytes.size()};
}

auto looks_like_pem(Bytes bytes) -> bool {
  return as_text(bytes).contains("-----BEGIN ");
}

/// Explains the most common reasons why OpenSSL rejects a PEM-encoded key.
auto pem_hint(Bytes bytes, bool is_private) -> Option<std::string> {
  auto const text = as_text(bytes);
  if (text.contains("ENCRYPTED")) {
    return "encrypted private keys are not supported";
  }
  if (not is_private and text.contains("PRIVATE KEY-----")) {
    return "pass the public key instead of the private key";
  }
  if (is_private and text.contains("PUBLIC KEY-----")) {
    return "decryption requires the private key";
  }
  return None{};
}

auto read_bio(Bytes bytes) -> BioPtr {
  if (bytes.size() > INT_MAX) {
    return nullptr;
  }
  return BioPtr{
    BIO_new_mem_buf(bytes.data(), static_cast<int>(bytes.size())),
  };
}

/// Picks the HPKE suite for a key: the key type determines the KEM, and the
/// KDF follows the hash function of the KEM. All suites use AES-256-GCM.
auto hpke_suite(EVP_PKEY* key) -> Option<OSSL_HPKE_SUITE> {
  auto const suite = [](uint16_t kem, uint16_t kdf) {
    return OSSL_HPKE_SUITE{kem, kdf, OSSL_HPKE_AEAD_ID_AES_GCM_256};
  };
  if (EVP_PKEY_is_a(key, "X25519")) {
    return suite(OSSL_HPKE_KEM_ID_X25519, OSSL_HPKE_KDF_ID_HKDF_SHA256);
  }
  if (EVP_PKEY_is_a(key, "X448")) {
    return suite(OSSL_HPKE_KEM_ID_X448, OSSL_HPKE_KDF_ID_HKDF_SHA512);
  }
  if (EVP_PKEY_is_a(key, "EC")) {
    auto group = std::array<char, 64>{};
    auto length = size_t{0};
    if (EVP_PKEY_get_utf8_string_param(key, OSSL_PKEY_PARAM_GROUP_NAME,
                                       group.data(), group.size(), &length)
        != 1) {
      return None{};
    }
    auto const name = std::string_view{group.data(), length};
    if (name == "prime256v1" or name == "P-256") {
      return suite(OSSL_HPKE_KEM_ID_P256, OSSL_HPKE_KDF_ID_HKDF_SHA256);
    }
    if (name == "secp384r1" or name == "P-384") {
      return suite(OSSL_HPKE_KEM_ID_P384, OSSL_HPKE_KDF_ID_HKDF_SHA384);
    }
    if (name == "secp521r1" or name == "P-521") {
      return suite(OSSL_HPKE_KEM_ID_P521, OSSL_HPKE_KDF_ID_HKDF_SHA512);
    }
  }
  return None{};
}

constexpr auto hpke_key_types
  = std::string_view{"X25519, X448, P-256, P-384, or P-521"};

/// Reads a raw X25519 or X448 key, or a PEM-encoded key of any supported
/// type, from `bytes`.
auto load_hpke_key(located<Bytes> key, bool is_private, diagnostic_handler& dh)
  -> failure_or<PkeyPtr> {
  auto const bytes = key.inner;
  auto const kind = std::string_view{is_private ? "private" : "public"};
  auto pkey = PkeyPtr{};
  if (looks_like_pem(bytes)) {
    auto bio = read_bio(bytes);
    if (not bio) {
      diagnostic::error("failed to read PEM-encoded {} key: {}", kind,
                        openssl_error())
        .primary(key.source)
        .emit(dh);
      return failure::promise();
    }
    pkey.reset(
      is_private
        ? PEM_read_bio_PrivateKey(bio.get(), nullptr, no_passphrase, nullptr)
        : PEM_read_bio_PUBKEY(bio.get(), nullptr, no_passphrase, nullptr));
    if (not pkey) {
      auto diag = diagnostic::error("failed to read PEM-encoded {} key: {}",
                                    kind, openssl_error())
                    .primary(key.source);
      if (auto hint = pem_hint(bytes, is_private)) {
        diag = std::move(diag).hint(std::move(*hint));
      }
      std::move(diag).emit(dh);
      return failure::promise();
    }
  } else if (bytes.size() == 32 or bytes.size() == 56) {
    auto const type = bytes.size() == 32 ? EVP_PKEY_X25519 : EVP_PKEY_X448;
    pkey.reset(is_private
                 ? EVP_PKEY_new_raw_private_key(type, nullptr, u8(bytes.data()),
                                                bytes.size())
                 : EVP_PKEY_new_raw_public_key(type, nullptr, u8(bytes.data()),
                                               bytes.size()));
    if (not pkey) {
      diagnostic::error("failed to read raw {} key: {}", kind, openssl_error())
        .primary(key.source)
        .emit(dh);
      return failure::promise();
    }
  } else {
    diagnostic::error("expected a PEM-encoded {} key, or a raw X25519 or X448 "
                      "key with 32 or 56 bytes, but got {} bytes",
                      kind, bytes.size())
      .primary(key.source)
      .hint("decode a raw key in hex or Base64 with `decode_hex` or "
            "`decode_base64`")
      .emit(dh);
    return failure::promise();
  }
  if (not hpke_suite(pkey.get())) {
    diagnostic::error("unsupported {} key type", kind)
      .primary(key.source)
      .note("supported key types are {}", hpke_key_types)
      .emit(dh);
    return failure::promise();
  }
  return pkey;
}

/// Encrypts `plaintext` into `encapsulated_key || ciphertext || tag` with a
/// single-shot HPKE context in base mode and with an empty `info`. OpenSSL
/// rejects empty plaintexts, so the caller must handle them.
auto hpke_seal(OSSL_HPKE_SUITE suite, std::span<unsigned char const> recipient,
               Bytes plaintext, Bytes aad) -> Option<blob> {
  auto ctx = HpkeCtxPtr{OSSL_HPKE_CTX_new(
    OSSL_HPKE_MODE_BASE, suite, OSSL_HPKE_ROLE_SENDER, nullptr, nullptr)};
  if (not ctx) {
    return None{};
  }
  auto const encap_size = OSSL_HPKE_get_public_encap_size(suite);
  auto const ciphertext_size
    = OSSL_HPKE_get_ciphertext_size(suite, plaintext.size());
  if (encap_size == 0 or ciphertext_size == 0) {
    return None{};
  }
  auto placeholder = Placeholder{};
  auto result = blob(encap_size + ciphertext_size);
  auto* out = u8(result.data());
  auto encap_length = encap_size;
  if (OSSL_HPKE_encap(ctx.get(), out, &encap_length, recipient.data(),
                      recipient.size(), nullptr, 0)
      != 1) {
    return None{};
  }
  auto ciphertext_length = ciphertext_size;
  if (OSSL_HPKE_seal(ctx.get(), out + encap_length, &ciphertext_length,
                     data_or(aad, placeholder), aad.size(),
                     data_or(plaintext, placeholder), plaintext.size())
      != 1) {
    return None{};
  }
  result.resize(encap_length + ciphertext_length);
  return result;
}

/// Decrypts the output of `hpke_seal`. The caller must ensure that `input`
/// holds more than the encapsulated key and the tag, because OpenSSL rejects
/// empty plaintexts.
auto hpke_open(OSSL_HPKE_SUITE suite, EVP_PKEY* key, Bytes input, Bytes aad)
  -> Result<blob, OpenError> {
  auto const encap_size = OSSL_HPKE_get_public_encap_size(suite);
  TENZIR_ASSERT(input.size() > encap_size + tag_size);
  auto ctx = HpkeCtxPtr{OSSL_HPKE_CTX_new(
    OSSL_HPKE_MODE_BASE, suite, OSSL_HPKE_ROLE_RECEIVER, nullptr, nullptr)};
  if (not ctx) {
    return Err{OpenError::setup};
  }
  // The decapsulation fails for an encapsulated key that is not a valid
  // public key, which means that someone modified the ciphertext.
  if (OSSL_HPKE_decap(ctx.get(), u8(input.data()), encap_size, key, nullptr, 0)
      != 1) {
    return Err{OpenError::authentication};
  }
  auto const ciphertext = input.subspan(encap_size);
  auto placeholder = Placeholder{};
  auto result = blob(ciphertext.size() - tag_size);
  auto length = result.size();
  if (OSSL_HPKE_open(ctx.get(), u8(result.data()), &length,
                     data_or(aad, placeholder), aad.size(),
                     u8(ciphertext.data()), ciphertext.size())
      != 1) {
    return Err{OpenError::authentication};
  }
  TENZIR_ASSERT(length == result.size());
  return result;
}

auto hpke_min_size(OSSL_HPKE_SUITE suite) -> size_t {
  return OSSL_HPKE_get_public_encap_size(suite) + tag_size;
}

struct HpkeEncryptArgs {
  nova::ValueArgument x;
  /// The public key needs no protection, so it may also be a plain value.
  nova::ConstantArgument public_key;
  Option<nova::ValueArgument> aad;
  location call;
  /// The suite and the encoded public key, derived in `validate`.
  OSSL_HPKE_SUITE suite{};
  std::vector<unsigned char> recipient;
};

class HpkeEncryptFunction {
public:
  static auto eval(HpkeEncryptArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto empty = nova::WarnOnce{};
    auto failed = nova::WarnOnce{};
    return apply_bytes(
      frame, "encrypt_hpke", args.x, args.aad, "aad", args.call,
      [&](diagnostic_handler& dh, Bytes input, Bytes aad,
          bool) -> Option<blob> {
        if (input.empty()) {
          empty(dh, diagnostic::warning("cannot encrypt empty values")
                      .primary(args.x.source)
                      .note("the HPKE implementation of OpenSSL does not "
                            "support empty plaintexts"));
          return None{};
        }
        auto result = hpke_seal(args.suite, args.recipient, input, aad);
        if (not result) {
          failed(dh,
                 diagnostic::warning("failed to encrypt: {}", openssl_error())
                   .primary(args.x.source));
        }
        return result;
      });
  }
};

class HpkeEncryptPlugin final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "encrypt_hpke";
  }

  auto is_deterministic() const -> bool override {
    // Every value gets a fresh ephemeral key.
    return false;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<HpkeEncryptArgs, HpkeEncryptFunction>{};
    d.positional("x", &HpkeEncryptArgs::x, "blob|string");
    d.named("public_key", &HpkeEncryptArgs::public_key, "blob|string|secret");
    d.named_optional("aad", &HpkeEncryptArgs::aad, "blob|string");
    d.call_location(&HpkeEncryptArgs::call);
    d.validate([](HpkeEncryptArgs& args,
                  diagnostic_handler& dh) -> failure_or<void> {
      auto bytes = Option<Bytes>{};
      if (auto const* x = try_as<nova::String>(&args.public_key.inner)) {
        bytes = bytes_of(std::string_view{*x});
      } else if (auto const* x = try_as<nova::Blob>(&args.public_key.inner)) {
        bytes = Bytes{*x};
      } else if (auto const* x = try_as<nova::Secret>(&args.public_key.inner)) {
        bytes = Bytes{x->data};
      } else {
        diagnostic::error("`public_key` must be a `blob`, `string`, or "
                          "`secret`")
          .primary(args.public_key.source)
          .emit(dh);
        return failure::promise();
      }
      TRY(auto key,
          load_hpke_key(located{*bytes, args.public_key.source}, false, dh));
      auto* encoded = static_cast<unsigned char*>(nullptr);
      auto const size = EVP_PKEY_get1_encoded_public_key(key.get(), &encoded);
      if (size == 0) {
        diagnostic::error("failed to encode public key: {}", openssl_error())
          .primary(args.public_key.source)
          .emit(dh);
        return failure::promise();
      }
      args.recipient.assign(encoded, encoded + size);
      OPENSSL_free(encoded);
      args.suite = *hpke_suite(key.get());
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    return requires_nova(name(), inv, ctx);
  }
};

struct HpkeDecryptArgs {
  nova::ValueArgument x;
  located<nova::Secret> private_key;
  Option<nova::ValueArgument> aad;
  location call;
  /// The suite and the parsed private key, derived in `validate`. OpenSSL
  /// keys are safe to use concurrently for operations that do not modify them.
  OSSL_HPKE_SUITE suite{};
  PkeyPtr key;
};

class HpkeDecryptFunction {
public:
  static auto eval(HpkeDecryptArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto too_short = nova::WarnOnce{};
    auto empty = nova::WarnOnce{};
    auto failed = nova::WarnOnce{};
    auto failed_setup = nova::WarnOnce{};
    auto const min_size = hpke_min_size(args.suite);
    return apply_bytes(
      frame, "decrypt_hpke", args.x, args.aad, "aad", args.call,
      [&](diagnostic_handler& dh, Bytes input, Bytes aad,
          bool is_string) -> Option<blob> {
        if (input.size() < min_size) {
          too_short(dh, with_encoding_hint(
                          diagnostic::warning("ciphertext is too short")
                            .primary(args.x.source)
                            .note("expected at least {} bytes", min_size),
                          is_string));
          return None{};
        }
        if (input.size() == min_size) {
          empty(dh, diagnostic::warning("cannot decrypt empty values")
                      .primary(args.x.source)
                      .note("the HPKE implementation of OpenSSL does not "
                            "support empty plaintexts"));
          return None{};
        }
        auto result = hpke_open(args.suite, args.key.get(), input, aad);
        if (result.is_err()) {
          auto const error = result.unwrap_err();
          auto& latch = error == OpenError::setup ? failed_setup : failed;
          latch(dh, open_warning(error,
                                 "the private key or the associated data does "
                                 "not match, or the ciphertext was modified",
                                 args.x.source, is_string));
          return None{};
        }
        return std::move(result).unwrap();
      });
  }
};

class HpkeDecryptPlugin final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "decrypt_hpke";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<HpkeDecryptArgs, HpkeDecryptFunction>{};
    d.positional("x", &HpkeDecryptArgs::x, "blob|string");
    d.named("private_key", &HpkeDecryptArgs::private_key);
    d.named_optional("aad", &HpkeDecryptArgs::aad, "blob|string");
    d.call_location(&HpkeDecryptArgs::call);
    d.validate(
      [](HpkeDecryptArgs& args, diagnostic_handler& dh) -> failure_or<void> {
        TRY(args.key, load_hpke_key(located{key_bytes(args.private_key),
                                            args.private_key.source},
                                    true, dh));
        args.suite = *hpke_suite(args.key.get());
        return {};
      });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    return requires_nova(name(), inv, ctx);
  }
};

// -- FF1 ----------------------------------------------------------------------

/// NIST SP 800-38G Rev. 1 requires a domain of at least one million values.
constexpr auto ff1_min_domain = uint64_t{1'000'000};

/// The maximum number of numerals, which NIST SP 800-38G leaves to the
/// implementation. The cost of FF1 grows quadratically with the length, so
/// the limit keeps a single long value from stalling a pipeline: a value at
/// the limit takes about 2 ms, but a value with 65,536 numerals takes 400 ms.
constexpr auto ff1_max_length = size_t{4'096};

/// The FF1 mode of NIST SP 800-38G for numeral strings in base `radix`. One
/// instance holds the working memory for a batch of values.
class Ff1 {
public:
  /// Takes an AES-ECB context that is keyed with the FF1 key and has padding
  /// disabled.
  Ff1(EVP_CIPHER_CTX* ecb, uint32_t radix)
    : ecb_{ecb},
      radix_{radix},
      ctx_{BN_CTX_new()},
      value_{BN_new()},
      y_{BN_new()},
      sum_{BN_new()},
      modulus_u_{BN_new()},
      modulus_v_{BN_new()} {
    // Keep the divisors of `str` below 2^63, so that a remainder of
    // `BN_div_word` never equals its error value.
    auto chunk_power = BnWord{1};
    while (chunk_power <= std::numeric_limits<BnWord>::max() / 2 / radix_) {
      chunk_power *= radix_;
      ++chunk_size_;
    }
  }

  auto valid() const -> bool {
    return ctx_ and value_ and y_ and sum_ and modulus_u_ and modulus_v_;
  }

  /// Encrypts or decrypts the numeral string `x` in place.
  auto apply(bool encrypt, std::span<uint16_t> x, Bytes tweak) -> bool {
    auto const n = x.size();
    auto const u = n / 2;
    auto const v = n - u;
    auto const t = tweak.size();
    // The limits keep the lengths within their 4-byte encodings in P, and the
    // byte length `b` below within `int`.
    TENZIR_ASSERT(n <= ff1_max_length);
    TENZIR_ASSERT(t <= UINT32_MAX);
    if (not power(modulus_u_.get(), u) or not power(modulus_v_.get(), v)) {
      return false;
    }
    // b = ceil(ceil(v * log2(radix)) / 8). The bit length of radix^v - 1
    // equals ceil(v * log2(radix)) exactly, without floating-point rounding.
    if (BN_copy(value_.get(), modulus_v_.get()) == nullptr
        or BN_sub_word(value_.get(), 1) != 1) {
      return false;
    }
    auto const b = (static_cast<size_t>(BN_num_bits(value_.get())) + 7) / 8;
    auto const d = 4 * ((b + 3) / 4) + 4;
    // P = [1]^1 || [2]^1 || [1]^1 || [radix]^3 || [10]^1 || [u mod 256]^1
    //     || [n]^4 || [t]^4
    auto p = Block{
      1,
      2,
      1,
      static_cast<unsigned char>(radix_ >> 16),
      static_cast<unsigned char>(radix_ >> 8),
      static_cast<unsigned char>(radix_),
      10,
      static_cast<unsigned char>(u % 256),
      static_cast<unsigned char>(n >> 24),
      static_cast<unsigned char>(n >> 16),
      static_cast<unsigned char>(n >> 8),
      static_cast<unsigned char>(n),
      static_cast<unsigned char>(t >> 24),
      static_cast<unsigned char>(t >> 16),
      static_cast<unsigned char>(t >> 8),
      static_cast<unsigned char>(t),
    };
    // The PRF is a CBC-MAC over P || Q. P is the same in every round, so we
    // encrypt it once and continue the chain from there.
    auto chain_p = Block{};
    if (not cipher(p, chain_p)) {
      return false;
    }
    // Q = T || [0]^((-t-b-1) mod 16) || [i]^1 || [NUM_radix(B)]^b
    auto const padding = (16 - (t + b + 1) % 16) % 16;
    q_.assign(t + padding + 1 + b, 0);
    if (t > 0) {
      std::memcpy(q_.data(), tweak.data(), t);
    }
    auto* const round_index = q_.data() + t + padding;
    auto* const numeral = round_index + 1;
    s_.assign((d + 15) / 16 * 16, 0);
    a_.assign(x.begin(), x.begin() + u);
    b_.assign(x.begin() + u, x.end());
    for (auto step = 0; step < 10; ++step) {
      auto const i = encrypt ? step : 9 - step;
      // Encryption feeds B into the round function and updates A,
      // decryption feeds A and updates B.
      auto& input = encrypt ? b_ : a_;
      auto& output = encrypt ? a_ : b_;
      *round_index = static_cast<unsigned char>(i);
      if (not num(input, value_.get())
          or BN_bn2binpad(value_.get(), numeral, static_cast<int>(b)) < 0) {
        return false;
      }
      auto r = chain_p;
      for (auto offset = size_t{0}; offset < q_.size(); offset += 16) {
        for (auto k = size_t{0}; k < 16; ++k) {
          r[k] ^= q_[offset + k];
        }
        if (not cipher(r, r)) {
          return false;
        }
      }
      // S = R || CIPH(R xor [1]^16) || CIPH(R xor [2]^16) || ..., truncated
      // to d bytes.
      std::memcpy(s_.data(), r.data(), 16);
      for (auto j = size_t{1}; j < s_.size() / 16; ++j) {
        auto block = r;
        for (auto k = size_t{0}; k < sizeof(j); ++k) {
          block[15 - k] ^= static_cast<unsigned char>(j >> (8 * k));
        }
        auto out = Block{};
        if (not cipher(block, out)) {
          return false;
        }
        std::memcpy(s_.data() + 16 * j, out.data(), 16);
      }
      if (BN_bin2bn(s_.data(), static_cast<int>(d), y_.get()) == nullptr) {
        return false;
      }
      auto const m = i % 2 == 0 ? u : v;
      auto const* modulus = i % 2 == 0 ? modulus_u_.get() : modulus_v_.get();
      // c = (NUM_radix(A) + y) mod radix^m when encrypting, and
      // c = (NUM_radix(B) - y) mod radix^m when decrypting.
      if (not num(output, value_.get())) {
        return false;
      }
      auto const combined = encrypt
                              ? BN_add(sum_.get(), value_.get(), y_.get())
                              : BN_sub(sum_.get(), value_.get(), y_.get());
      if (combined != 1
          or BN_nnmod(value_.get(), sum_.get(), modulus, ctx_.get()) != 1) {
        return false;
      }
      // The output half becomes the input half of the next round, and the
      // input half receives C = STR^m_radix(c).
      std::swap(input, output);
      input.resize(m);
      if (not str(value_.get(), input)) {
        return false;
      }
    }
    std::ranges::copy(a_, x.begin());
    std::ranges::copy(b_, x.begin() + static_cast<ptrdiff_t>(u));
    return true;
  }

private:
  using Block = std::array<unsigned char, 16>;

  auto cipher(Block const& in, Block& out) -> bool {
    auto length = 0;
    return EVP_EncryptUpdate(ecb_, out.data(), &length, in.data(), 16) == 1
           and length == 16;
  }

  /// Computes `radix^exponent`.
  auto power(BIGNUM* result, size_t exponent) -> bool {
    BN_CTX_start(ctx_.get());
    auto* base = BN_CTX_get(ctx_.get());
    auto* bn_exponent = BN_CTX_get(ctx_.get());
    auto const success = bn_exponent != nullptr
                         and BN_set_word(base, radix_) == 1
                         and BN_set_word(bn_exponent, exponent) == 1
                         and BN_exp(result, base, bn_exponent, ctx_.get()) == 1;
    BN_CTX_end(ctx_.get());
    return success;
  }

  /// Computes NUM_radix(x), with the most significant numeral first. The
  /// conversion takes `chunk_size_` numerals at a time, which divides the
  /// number of quadratic big number operations by that factor.
  auto num(std::span<uint16_t const> x, BIGNUM* result) -> bool {
    BN_zero(result);
    while (not x.empty()) {
      auto const size = std::min(x.size(), chunk_size_);
      auto chunk = BnWord{0};
      auto factor = BnWord{1};
      for (auto digit : x.first(size)) {
        chunk = chunk * radix_ + digit;
        factor *= radix_;
      }
      if (BN_mul_word(result, factor) != 1 or BN_add_word(result, chunk) != 1) {
        return false;
      }
      x = x.subspan(size);
    }
    return true;
  }

  /// Computes STR^m_radix(value) for `m = out.size()`, taking `chunk_size_`
  /// numerals at a time. Consumes `value`.
  auto str(BIGNUM* value, std::span<uint16_t> out) -> bool {
    while (not out.empty()) {
      auto const size = std::min(out.size(), chunk_size_);
      auto divisor = BnWord{1};
      for (auto k = size_t{0}; k < size; ++k) {
        divisor *= radix_;
      }
      auto chunk = BN_div_word(value, divisor);
      if (chunk == static_cast<BnWord>(-1)) {
        return false;
      }
      for (auto& digit : out.last(size) | std::views::reverse) {
        digit = static_cast<uint16_t>(chunk % radix_);
        chunk /= radix_;
      }
      out = out.first(out.size() - size);
    }
    return true;
  }

  EVP_CIPHER_CTX* ecb_;
  uint32_t radix_;
  /// The number of numerals that fit into one word.
  size_t chunk_size_ = 0;
  BnCtxPtr ctx_;
  BnPtr value_;
  BnPtr y_;
  BnPtr sum_;
  BnPtr modulus_u_;
  BnPtr modulus_v_;
  std::vector<unsigned char> q_;
  std::vector<unsigned char> s_;
  std::vector<uint16_t> a_;
  std::vector<uint16_t> b_;
};

struct Ff1Args {
  nova::ValueArgument x;
  located<nova::Secret> key;
  located<std::string> alphabet{"0123456789", location::unknown};
  Option<nova::ValueArgument> tweak;
  location call;
  /// The AES-ECB cipher, the numeral of every ASCII character (or -1), and
  /// the minimum number of numerals, all derived in `validate`.
  std::string cipher;
  std::array<int16_t, 128> numerals{};
  size_t min_length = 0;
};

template <bool Encrypt>
class Ff1Function {
public:
  static auto eval(Ff1Args const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto const name = Encrypt ? "encrypt_ff1" : "decrypt_ff1";
    auto cipher = fetch_cipher(args.cipher);
    auto ctx = CipherCtxPtr{EVP_CIPHER_CTX_new()};
    auto const key = key_bytes(args.key);
    if (not cipher or not ctx
        or EVP_EncryptInit_ex2(ctx.get(), cipher.get(), u8(key.data()), nullptr,
                               nullptr)
             != 1
        or EVP_CIPHER_CTX_set_padding(ctx.get(), 0) != 1) {
      diagnostic::warning("failed to initialize `{}`: {}", name,
                          openssl_error())
        .primary(args.call)
        .emit(frame);
      return frame.null();
    }
    auto const radix = static_cast<uint32_t>(args.alphabet.inner.size());
    auto ff1 = Ff1{ctx.get(), radix};
    if (not ff1.valid()) {
      diagnostic::warning("failed to initialize `{}`", name)
        .primary(args.call)
        .emit(frame);
      return frame.null();
    }
    auto numerals = std::vector<uint16_t>{};
    auto too_short = nova::WarnOnce{};
    auto too_long = nova::WarnOnce{};
    auto tweak_too_long = nova::WarnOnce{};
    auto null_tweak = nova::WarnOnce{};
    auto failed = nova::WarnOnce{};
    auto const transform = [&](diagnostic_handler& dh, std::string_view input,
                               Bytes tweak) -> Option<std::string> {
      // Characters outside of the alphabet pass through unchanged. All
      // alphabet characters are ASCII, so this never splits a multi-byte
      // UTF-8 sequence.
      numerals.clear();
      for (auto c : input) {
        auto const byte = static_cast<unsigned char>(c);
        if (byte < args.numerals.size() and args.numerals[byte] >= 0) {
          numerals.push_back(static_cast<uint16_t>(args.numerals[byte]));
        }
      }
      if (numerals.size() < args.min_length) {
        too_short(dh, diagnostic::warning("value has too few characters from "
                                          "the alphabet")
                        .primary(args.x.source)
                        .note("FF1 requires at least {} characters from the "
                              "alphabet with {} characters",
                              args.min_length, radix));
        return None{};
      }
      if (numerals.size() > ff1_max_length) {
        too_long(dh, diagnostic::warning("value has too many characters from "
                                         "the alphabet")
                       .primary(args.x.source)
                       .note("FF1 supports at most {} characters from the "
                             "alphabet",
                             ff1_max_length));
        return None{};
      }
      if (tweak.size() > UINT32_MAX) {
        tweak_too_long(dh, diagnostic::warning("`tweak` is too long")
                             .primary(args.tweak->source)
                             .note("FF1 supports tweaks of at most {} bytes",
                                   UINT32_MAX));
        return None{};
      }
      if (not ff1.apply(Encrypt, numerals, tweak)) {
        failed(dh, diagnostic::warning("failed to {}: {}",
                                       Encrypt ? "encrypt" : "decrypt",
                                       openssl_error())
                     .primary(args.x.source));
        return None{};
      }
      auto result = std::string{input};
      auto next = numerals.begin();
      for (auto& c : result) {
        auto const byte = static_cast<unsigned char>(c);
        if (byte < args.numerals.size() and args.numerals[byte] >= 0) {
          c = args.alphabet.inner[*next++];
        }
      }
      return result;
    };
    if (not args.tweak) {
      return nova::apply_kernel<1>(
        frame, name, std::array<nova::ValueArgument, 1>{args.x}, args.x.source,
        [&]<std::same_as<std::string_view> X>(diagnostic_handler& dh, X input) {
          return transform(dh, input, Bytes{});
        });
    }
    return nova::apply_kernel<2>(
      frame, name, std::array<nova::ValueArgument, 2>{args.x, *args.tweak},
      args.call,
      detail::overload{
        [&]<std::same_as<std::string_view> X, BytesView T>(
          diagnostic_handler& dh, X input, T tweak) {
          return transform(dh, input, bytes_of(tweak));
        },
        [&]<std::same_as<std::string_view> X>(
          diagnostic_handler& dh, X, nova::Null) -> Option<std::string> {
          null_tweak(dh, null_argument_warning("tweak", args.tweak->source));
          return None{};
        },
      });
  }
};

template <bool Encrypt>
class Ff1Plugin final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return Encrypt ? "encrypt_ff1" : "decrypt_ff1";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<Ff1Args, Ff1Function<Encrypt>>{};
    d.positional("x", &Ff1Args::x, "string");
    d.named("key", &Ff1Args::key);
    d.named_optional("alphabet", &Ff1Args::alphabet);
    d.named_optional("tweak", &Ff1Args::tweak, "blob|string");
    d.call_location(&Ff1Args::call);
    d.validate([](Ff1Args& args, diagnostic_handler& dh) -> failure_or<void> {
      auto const key_size = args.key.inner.data.size();
      if (key_size != 16 and key_size != 24 and key_size != 32) {
        diagnostic::error("`key` must have 16, 24, or 32 bytes, but has {} "
                          "bytes",
                          key_size)
          .primary(args.key.source)
          .hint("decode a hex or Base64 key with `decode_hex` or "
                "`decode_base64`, for example "
                "`secret(\"key\").decode_hex()`")
          .emit(dh);
        return failure::promise();
      }
      args.cipher = fmt::format("AES-{}-ECB", key_size * 8);
      auto const& alphabet = args.alphabet.inner;
      if (alphabet.size() < 2) {
        diagnostic::error("`alphabet` must have at least 2 characters")
          .primary(args.alphabet)
          .emit(dh);
        return failure::promise();
      }
      args.numerals.fill(-1);
      for (auto i = size_t{0}; i < alphabet.size(); ++i) {
        auto const byte = static_cast<unsigned char>(alphabet[i]);
        if (byte >= args.numerals.size()) {
          diagnostic::error("`alphabet` must consist of ASCII characters")
            .primary(args.alphabet)
            .emit(dh);
          return failure::promise();
        }
        if (args.numerals[byte] >= 0) {
          diagnostic::error("`alphabet` contains `{}` more than once",
                            alphabet[i])
            .primary(args.alphabet)
            .emit(dh);
          return failure::promise();
        }
        args.numerals[byte] = static_cast<int16_t>(i);
      }
      // The smallest length n >= 2 with radix^n >= one million.
      auto const radix = uint64_t{alphabet.size()};
      args.min_length = 2;
      for (auto domain = radix * radix; domain < ff1_min_domain; domain
                                                                 *= radix) {
        ++args.min_length;
      }
      if (not fetch_cipher(args.cipher)) {
        diagnostic::error("cipher `{}` is not available: {}", args.cipher,
                          openssl_error())
          .primary(args.call)
          .emit(dh);
        return failure::promise();
      }
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    return requires_nova(name(), inv, ctx);
  }
};

} // namespace

} // namespace tenzir::plugins::encryption

TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::AesPlugin<
                       tenzir::plugins::encryption::AesMode::gcm, true>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::AesPlugin<
                       tenzir::plugins::encryption::AesMode::gcm, false>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::AesPlugin<
                       tenzir::plugins::encryption::AesMode::gcm_siv, true>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::AesPlugin<
                       tenzir::plugins::encryption::AesMode::gcm_siv, false>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::AesPlugin<
                       tenzir::plugins::encryption::AesMode::siv, true>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::AesPlugin<
                       tenzir::plugins::encryption::AesMode::siv, false>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::HpkeEncryptPlugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::HpkeDecryptPlugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::Ff1Plugin<true>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::encryption::Ff1Plugin<false>)
