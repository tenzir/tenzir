//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"

#include <tenzir/detail/base58.hpp>
#include <tenzir/ecc.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/try.hpp>

#include <boost/algorithm/hex.hpp>
#include <openssl/bn.h>
#include <openssl/core_names.h>
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/param_build.h>
#include <openssl/params.h>
#include <openssl/rand.h>

#include <memory>
#include <simdjson.h>
#include <string>

namespace tenzir::ecc {

void cleanse_memory(void* start, size_t size) {
  OPENSSL_cleanse(start, size);
}

// Helper macro to declare an OpenSSL object together with its
// associated destructor function.
#define DECLARE(decl, init, dtor)                                              \
  auto decl = init;                                                            \
  if (! decl) {                                                                \
    return caf::make_error(ec::system_error, #init);                           \
  }                                                                            \
  auto decl##_guard                                                            \
    = std::unique_ptr<std::remove_reference_t<decltype(*decl)>,                \
                      decltype(&dtor)>(decl, &dtor);

// OpenSSL randomly switches between using 0, 1, nullptr and <= 0 to indicate
// failure, so we can't use the generic TRY macro here.
// TODO: Improve the error messages by using diagnostics, and using
// `ERR_print_errors_fp()` to get the internal error message.
#define CHECK_EQ_1(expr)                                                       \
  if (! ((expr) == 1)) {                                                       \
    return caf::make_error(ec::system_error, #expr);                           \
  }

#define CHECK_N_0(expr)                                                        \
  if ((expr) == 0) {                                                           \
    return caf::make_error(ec::system_error, #expr);                           \
  }

#define CHECK_GT_0(expr)                                                       \
  if ((expr) <= 0) {                                                           \
    return caf::make_error(ec::system_error, #expr);                           \
  }

#define CHECK_N_NULL(expr)                                                     \
  if ((expr) == nullptr) {                                                     \
    return caf::make_error(ec::system_error, #expr);                           \
  }

// All functions in here operate on the secp256k1 curve and it's
// associated group; there is no API flexibility on this by design.
constexpr size_t point_size = 65;
constexpr size_t compressed_point_size = 33;
constexpr size_t private_key_bits = 256;
constexpr size_t nonce_length = 16;
constexpr size_t tag_length = 16;
constexpr size_t block_size_bytes = 16;

static auto get_group() -> caf::expected<const EC_GROUP*> {
  static auto group_holder
    = std::unique_ptr<EC_GROUP, decltype(&EC_GROUP_free)>(
      EC_GROUP_new_by_curve_name(NID_secp256k1), EC_GROUP_free);
  return group_holder.get();
}

static auto point_to_bytes(EC_POINT* point) -> std::string {
  auto result = std::string{};
  auto group = get_group();
  TENZIR_ASSERT(group);
  auto* bignum_ctx = BN_CTX_new();
  auto required_size = EC_POINT_point2oct(
    *group, point, POINT_CONVERSION_UNCOMPRESSED, nullptr, 0, bignum_ctx);
  TENZIR_ASSERT(required_size == point_size);
  result.resize(point_size);
  auto n = EC_POINT_point2oct(*group, point, POINT_CONVERSION_UNCOMPRESSED,
                              reinterpret_cast<unsigned char*>(result.data()),
                              result.size(), bignum_ctx);
  TENZIR_ASSERT(n != 0);
  BN_CTX_free(bignum_ctx);
  return result;
}

static auto bignum_to_hex(const BIGNUM* bn) -> std::string {
  auto* bn_hex = BN_bn2hex(bn);
  auto result = std::string{bn_hex};
  OPENSSL_free(bn_hex);
  return result;
}

template <typename ByteBuffer>
static auto bytes_to_hex(const ByteBuffer& b) -> std::string {
  auto result = std::string{};
  for (const auto& x : b) {
    result += fmt::format("{:02x}", reinterpret_cast<unsigned char const&>(x));
  }
  return result;
}

auto string_keypair::from_private_key(std::string&& private_key)
  -> caf::expected<string_keypair> {
  TRY(auto group, get_group());
  DECLARE(secret_number, BN_new(), BN_clear_free);
  // We can't use a string_view as the argument, because we can't be sure
  // it is null-terminated, which is expected by OpenSSL here.
  CHECK_N_0(BN_hex2bn(&secret_number, private_key.c_str()));
  DECLARE(bignum_ctx, BN_CTX_new(), BN_CTX_free);
  DECLARE(public_point, EC_POINT_new(group), EC_POINT_free);
  CHECK_EQ_1(EC_POINT_mul(group, public_point, secret_number, nullptr, nullptr,
                          bignum_ctx));
  auto public_key_bytes = point_to_bytes(public_point);
  return string_keypair{
    .private_key = std::move(private_key),
    .public_key = bytes_to_hex(public_key_bytes),
  };
}

auto generate_keypair() -> caf::expected<string_keypair> {
  auto result = string_keypair{};
  // Generate keypair.
  DECLARE(openssl_keypair,
          EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "secp256k1"),
          EVP_PKEY_free);
  CHECK_EQ_1(EVP_PKEY_set_utf8_string_param(
    openssl_keypair, OSSL_PKEY_PARAM_EC_POINT_CONVERSION_FORMAT, "compressed"));
  // Extract private key
  DECLARE(private_key, BN_new(), BN_clear_free);
  CHECK_EQ_1(EVP_PKEY_get_bn_param(openssl_keypair, OSSL_PKEY_PARAM_PRIV_KEY,
                                   &private_key));
  result.private_key = bignum_to_hex(private_key);
  // Extract public key
  auto public_key = std::array<unsigned char, compressed_point_size>{};
  auto pubkey_len = size_t{0};
  CHECK_EQ_1(EVP_PKEY_get_octet_string_param(
    openssl_keypair, OSSL_PKEY_PARAM_PUB_KEY, public_key.data(),
    public_key.size(), &pubkey_len));
  TENZIR_ASSERT(pubkey_len == compressed_point_size);
  result.public_key = bytes_to_hex(public_key);
  return result;
}

// A simplified interface to the OpenSSL implementation of HKDF that expands
// the given input `key` (which must be a high-entropy string) into 32 bytes
// of uniform random data.
static auto hkdf(std::string_view key)
  -> caf::expected<std::array<unsigned char, 32>> {
  DECLARE(kdf, EVP_KDF_fetch(nullptr, "hkdf", nullptr), EVP_KDF_free);
  DECLARE(kctx, EVP_KDF_CTX_new(kdf), EVP_KDF_CTX_free);
  constexpr auto digest = std::string_view{"sha256"};
  auto params = std::array<OSSL_PARAM, 3>{
    OSSL_PARAM_construct_utf8_string("digest", const_cast<char*>(digest.data()),
                                     digest.size()),
    OSSL_PARAM_construct_octet_string("key", const_cast<char*>(key.data()),
                                      key.size()),
    OSSL_PARAM_construct_end(),
  };
  CHECK_GT_0(EVP_KDF_CTX_set_params(kctx, params.data()));
  auto derived = std::array<unsigned char, 32>{};
  CHECK_GT_0(EVP_KDF_derive(kctx, derived.data(), derived.size(), nullptr));
  return derived;
}

// Encrypt text using the given public_key using ECIES.
auto encrypt(std::string_view plaintext, const std::string_view public_key)
  -> caf::expected<std::string> {
  // Create a new ephemeral keypair.
  DECLARE(ephemeral_private, BN_new(), BN_clear_free);
  BN_rand(ephemeral_private, private_key_bits, BN_RAND_TOP_ANY,
          BN_RAND_BOTTOM_ANY);
  TRY(auto group, get_group());
  DECLARE(bignum_ctx, BN_CTX_new(), BN_CTX_free);
  DECLARE(ephemeral_public, EC_POINT_new(group), EC_POINT_free);
  CHECK_EQ_1(EC_POINT_mul(group, ephemeral_public, ephemeral_private, nullptr,
                          nullptr, bignum_ctx));
  // Compute the shared point as `ephemeral_private * transport_key`
  DECLARE(shared_point, EC_POINT_new(group), EC_POINT_free);
  DECLARE(public_point, EC_POINT_new(group), EC_POINT_free);
  CHECK_N_NULL(
    EC_POINT_hex2point(group, public_key.data(), public_point, bignum_ctx));
  CHECK_EQ_1(EC_POINT_mul(group, shared_point, nullptr, public_point,
                          ephemeral_private, bignum_ctx));
  // Derive the AES key from the shared point.
  auto public_str = point_to_bytes(ephemeral_public);
  auto shared_str = point_to_bytes(shared_point);
  auto master = public_str + shared_str;
  TRY(auto derived, hkdf(master));
  // Perform the actual AES encryption.
  auto nonce = std::array<unsigned char, nonce_length>{};
  CHECK_EQ_1(RAND_bytes(nonce.data(), nonce_length));
  DECLARE(cipher_ctx, EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
  DECLARE(cipher, EVP_CIPHER_fetch(nullptr, "AES-256-GCM", nullptr),
          EVP_CIPHER_free);
  auto gcm_ivlen = size_t{16};
  auto aes_params = std::array<OSSL_PARAM, 2>{
    OSSL_PARAM_construct_size_t(OSSL_CIPHER_PARAM_AEAD_IVLEN, &gcm_ivlen),
    OSSL_PARAM_END};
  CHECK_EQ_1(EVP_EncryptInit_ex2(cipher_ctx, cipher, derived.data(),
                                 nonce.data(), aes_params.data()));
  auto ciphertext = std::string{};
  ciphertext.resize(plaintext.size() + 2 * block_size_bytes);
  auto* out_ptr = reinterpret_cast<unsigned char*>(ciphertext.data());
  int out_length = ciphertext.size();
  CHECK_EQ_1(
    EVP_EncryptUpdate(cipher_ctx, out_ptr, &out_length,
                      reinterpret_cast<const unsigned char*>(plaintext.data()),
                      plaintext.size()));
  int tmplen = 0;
  CHECK_EQ_1(EVP_EncryptFinal_ex(
    cipher_ctx, reinterpret_cast<unsigned char*>(ciphertext.data()), &tmplen));
  ciphertext.resize(out_length + tmplen);

  auto tag = std::array<unsigned char, tag_length>{};
  auto tag_params = std::array<OSSL_PARAM, 2>{
    OSSL_PARAM_construct_octet_string(OSSL_CIPHER_PARAM_AEAD_TAG, tag.data(),
                                      tag.size()),
    OSSL_PARAM_END};
  CHECK_EQ_1(EVP_CIPHER_CTX_get_params(cipher_ctx, tag_params.data()));
  // Concatenate the various parts to return the ECIES encrypted
  // string as `base64(public_key | nonce | tag | ciphertext)`
  auto combined_bytes = std::string{};
  combined_bytes.reserve(point_size + nonce_length + tag_length
                         + ciphertext.size());
  combined_bytes.resize(point_size + nonce_length + tag_length);
  CHECK_N_0(
    EC_POINT_point2oct(group, ephemeral_public, POINT_CONVERSION_UNCOMPRESSED,
                       reinterpret_cast<unsigned char*>(combined_bytes.data()),
                       point_size, bignum_ctx));
  std::memcpy(combined_bytes.data() + point_size, nonce.data(), nonce_length);
  std::memcpy(combined_bytes.data() + point_size + nonce_length, tag.data(),
              tag_length);
  combined_bytes += ciphertext;
  return detail::base58::encode(combined_bytes);
}

auto decrypt(std::string_view base58_ciphertext, const string_keypair& keypair)
  -> caf::expected<cleansing_blob> {
  // ciphertext  =   ephemeral_key   | nonce (iv) | tag  | cipherdata
  // bytes                 65        |   16       |  16  |   ..rest
  //
  // Decode the input.
  TRY(auto raw_ciphertext, detail::base58::decode(base58_ciphertext));
  auto minimum_message_size = point_size + nonce_length + tag_length;
  if (raw_ciphertext.size() < minimum_message_size) {
    return diagnostic::error("invalid cipher: too short")
      .note("expected `{}` bytes, but got only `{}`", minimum_message_size,
            raw_ciphertext.size())
      .to_error();
  }
  auto ephemeral_key = std::array<unsigned char, point_size>{};
  std::copy_n(raw_ciphertext.data(), point_size, ephemeral_key.begin());
  auto nonce = std::array<unsigned char, nonce_length>{};
  std::copy_n(raw_ciphertext.data() + point_size, nonce_length, nonce.begin());
  auto tag = std::array<unsigned char, tag_length>{};
  std::copy_n(raw_ciphertext.data() + point_size + nonce_length, tag_length,
              tag.begin());
  auto cipher = std::vector<unsigned char>(raw_ciphertext.begin() + point_size
                                             + nonce_length + tag_length,
                                           raw_ciphertext.end());
  // Set up required objects.
  TRY(auto group, get_group());
  DECLARE(bignum_ctx, BN_CTX_new(), BN_CTX_free);
  DECLARE(cipher_ctx, EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
  DECLARE(secret_number, BN_new(), BN_clear_free);
  DECLARE(public_point, EC_POINT_new(group), EC_POINT_free);
  DECLARE(shared_point, EC_POINT_new(group), EC_POINT_free);
  // Initialize the points
  CHECK_N_0(BN_hex2bn(&secret_number, keypair.private_key.c_str()));
  CHECK_EQ_1(EC_POINT_oct2point(group, public_point, ephemeral_key.data(),
                                ephemeral_key.size(), bignum_ctx));
  CHECK_EQ_1(EC_POINT_is_on_curve(group, public_point, bignum_ctx));
  CHECK_EQ_1(EC_POINT_mul(group, shared_point, nullptr, public_point,
                          secret_number, bignum_ctx));
  // Derive shared AES key.
  auto public_bytes = point_to_bytes(public_point);
  auto shared_bytes = point_to_bytes(shared_point);
  auto bytes = public_bytes + shared_bytes;
  TRY(auto shared_secret, hkdf(bytes));
  // Perform AES decryption.
  auto plaintext = cleansing_blob{};
  plaintext.resize(base58_ciphertext.size());
  auto len = static_cast<int>(plaintext.size());
  // NB: It's not clear why we need to set up encryption here, but
  // decryption fails without the call.
  CHECK_EQ_1(EVP_EncryptInit_ex2(cipher_ctx, EVP_aes_256_gcm(), nullptr,
                                 nullptr, nullptr));
  CHECK_EQ_1(
    EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_IVLEN, 16, nullptr));
  CHECK_EQ_1(EVP_DecryptInit_ex(cipher_ctx, nullptr, nullptr,
                                shared_secret.data(), nonce.data()));
  CHECK_EQ_1(EVP_DecryptUpdate(
    cipher_ctx, reinterpret_cast<unsigned char*>(plaintext.data()), &len,
    cipher.data(), cipher.size()));
  auto total_len = len;
  CHECK_EQ_1(EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_AEAD_SET_TAG, tag.size(),
                                 tag.data()));
  // Finalize the decryption session. This checks the AEAD tag matches.
  CHECK_EQ_1(EVP_DecryptFinal_ex(
    cipher_ctx, reinterpret_cast<unsigned char*>(plaintext.data() + len),
    &len));
  total_len += len;
  plaintext.resize(total_len);
  return plaintext;
}

auto decrypt_string(std::string_view base58_ciphertext,
                    const string_keypair& keypair)
  -> caf::expected<cleansing_string> {
  TRY(auto blob, decrypt(base58_ciphertext, keypair));
  auto str = std::string_view{reinterpret_cast<const char*>(blob.data()),
                              reinterpret_cast<const char*>(blob.data())
                                + blob.size()};
  if (! simdjson::validate_utf8(str.begin(), str.size())) {
    return diagnostic::error("invalid string").to_error();
  }
  return cleansing_string{str};
}

} // namespace tenzir::ecc
