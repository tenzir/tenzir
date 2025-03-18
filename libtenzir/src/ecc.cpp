//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/base64.hpp>
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

#include <random>
#include <string>

namespace tenzir::ecc {

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
// failure, so we can't use the generic TRY macro here
#define CHECK_EQ_1(expr)                                                       \
  if (! (expr == 1)) {                                                         \
    return caf::make_error(ec::system_error, #expr);                           \
  }

#define CHECK_N_0(expr)                                                        \
  if (expr == 0) {                                                             \
    return caf::make_error(ec::system_error, #expr);                           \
  }

#define CHECK_GT_0(expr)                                                       \
  if (expr <= 0) {                                                             \
    return caf::make_error(ec::system_error, #expr);                           \
  }

#define CHECK_N_NULL(expr)                                                     \
  if (expr == nullptr) {                                                       \
    return caf::make_error(ec::system_error, #expr);                           \
  }

string_keypair::~string_keypair() {
  OPENSSL_cleanse(public_key.data(), public_key.size());
  OPENSSL_cleanse(private_key.data(), private_key.size());
}

auto point_to_hex(const EC_GROUP* group, EC_POINT* point,
                  BN_CTX* ctx) -> std::string {
  auto ptr = EC_POINT_point2hex(group, point, POINT_CONVERSION_COMPRESSED, ctx);
  TENZIR_ASSERT(ptr);
  auto res = std::string{ptr};
  OPENSSL_free(ptr);
  return res;
}

auto generate_keypair() -> caf::expected<string_keypair> {
  auto result = string_keypair{};
  // Generate keypair in OpenSSL.
  DECLARE(openssl_keypair,
          EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "secp256k1"),
          EVP_PKEY_free);
  CHECK_EQ_1(EVP_PKEY_set_utf8_string_param(
    openssl_keypair, OSSL_PKEY_PARAM_EC_POINT_CONVERSION_FORMAT, "compressed"));
  // Extract private key
  DECLARE(private_key, BN_new(), BN_clear_free);
  CHECK_EQ_1(EVP_PKEY_get_bn_param(openssl_keypair, OSSL_PKEY_PARAM_PRIV_KEY,
                                   &private_key));
  auto private_key_hex = BN_bn2hex(private_key);
  result.private_key = std::string{private_key_hex};
  OPENSSL_free(private_key_hex);
  // Extract public key
  auto public_key = std::array<unsigned char, 80>{};
  auto pubkey_len = size_t{0};
  CHECK_EQ_1(EVP_PKEY_get_octet_string_param(
    openssl_keypair, OSSL_PKEY_PARAM_PUB_KEY, public_key.data(),
    public_key.size(), &pubkey_len));
  auto public_key_hex
    = fmt::format("{:02x}", fmt::join(public_key.begin(),
                                      public_key.begin() + pubkey_len, ""));
  result.public_key = std::move(public_key_hex);
  return result;
}

// A simplified interface to the OpenSSL implementation of HKDF that expands
// the given input `key` (which must be a high-entropy string) into 32 bytes
// of uniform random data.
static auto
hkdf(std::string_view key) -> caf::expected<std::array<unsigned char, 32>> {
  DECLARE(kdf, EVP_KDF_fetch(NULL, "hkdf", NULL), EVP_KDF_free);
  DECLARE(kctx, EVP_KDF_CTX_new(kdf), EVP_KDF_CTX_free);
  OSSL_PARAM params[3], *p = params;
  constexpr auto digest = std::string_view{"sha256"};
  *p++ = OSSL_PARAM_construct_utf8_string(
    "digest", const_cast<char*>(digest.data()), digest.size());
  *p++ = OSSL_PARAM_construct_octet_string("key", const_cast<char*>(key.data()),
                                           key.size());
  *p = OSSL_PARAM_construct_end();
  CHECK_GT_0(EVP_KDF_CTX_set_params(kctx, params));
  auto derived = std::array<unsigned char, 32>{};
  CHECK_GT_0(EVP_KDF_derive(kctx, derived.data(), derived.size(), NULL));
  return derived;
}

// Encrypt text using the given public_key
auto encrypt(std::string_view plain_text, const std::string_view transport_key)
  -> caf::expected<std::string> {
  DECLARE(openssl_keypair,
          EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "secp256k1"),
          EVP_PKEY_free);
  CHECK_EQ_1(EVP_PKEY_set_utf8_string_param(
    openssl_keypair, OSSL_PKEY_PARAM_EC_POINT_CONVERSION_FORMAT, "compressed"));
  // Extract private key
  DECLARE(private_key, BN_new(), BN_clear_free);
  CHECK_EQ_1(EVP_PKEY_get_bn_param(openssl_keypair, OSSL_PKEY_PARAM_PRIV_KEY,
                                   &private_key));
  // shared_point = peer_public_key.multiply(private_key.secret)
  DECLARE(group, EC_GROUP_new_by_curve_name(NID_secp256k1), EC_GROUP_free);
  DECLARE(shared_point, EC_POINT_new(group), EC_POINT_free);
  DECLARE(public_key, EC_POINT_new(group), EC_POINT_free);
  DECLARE(bignum_ctx, BN_CTX_new(), BN_CTX_free);
  DECLARE(transport_point, EC_POINT_new(group), EC_POINT_free);
  CHECK_N_NULL(EC_POINT_hex2point(group, transport_key.data(), transport_point,
                                  bignum_ctx));
  CHECK_EQ_1(EC_POINT_mul(group, shared_point, nullptr, transport_point,
                          private_key, bignum_ctx));
  CHECK_EQ_1(
    EC_POINT_mul(group, public_key, private_key, nullptr, nullptr, bignum_ctx));
  // master = private_key.public_key.format(is_compressed) + shared_point.format(
  //     is_compressed
  // )
  auto public_str = point_to_hex(group, public_key, bignum_ctx);
  auto shared_str = point_to_hex(group, shared_point, bignum_ctx);
  auto master = public_str + shared_str;
  // return derive_key(master)
  TRY(auto derived, hkdf(master));
  //       nonce = os.urandom(nonce_length)
  constexpr static size_t nonce_length = 16;
  std::array<unsigned char, nonce_length> nonce;
  CHECK_EQ_1(RAND_bytes(nonce.data(), nonce_length));
  // aes_cipher = AES.new(key, AES.MODE_GCM, nonce)
  DECLARE(cipher_ctx, EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
  CHECK_EQ_1(EVP_EncryptInit_ex2(cipher_ctx, EVP_aes_256_gcm(), nullptr,
                                 nullptr, nullptr));
  CHECK_EQ_1(EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_IVLEN,
                                 nonce_length, nullptr));
  CHECK_EQ_1(EVP_EncryptInit_ex2(cipher_ctx, EVP_aes_256_gcm(), derived.data(),
                                 nonce.data(), nullptr));
  // encrypted, tag = aes_cipher.encrypt_and_digest(plain_text)
  auto cipher_text = std::string{};
  cipher_text.resize(plain_text.size()
                     + 2 * EVP_CIPHER_CTX_get_block_size(cipher_ctx));
  auto out_ptr = reinterpret_cast<unsigned char*>(cipher_text.data());
  int out_length = cipher_text.size();
  CHECK_EQ_1(
    EVP_EncryptUpdate(cipher_ctx, out_ptr, &out_length,
                      reinterpret_cast<const unsigned char*>(plain_text.data()),
                      plain_text.size()));
  out_ptr += out_length;
  int final_add_length = 0;
  CHECK_EQ_1(EVP_EncryptFinal_ex(cipher_ctx, out_ptr, &final_add_length));
  cipher_text.resize(out_length + final_add_length);
  constexpr static size_t tag_length = 16;
  std::array<unsigned char, tag_length> tag;
  CHECK_EQ_1(EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_GET_TAG, tag_length,
                                 tag.data()));
  auto combined_bytes = std::string{};
  constexpr size_t point_size = 65;
  combined_bytes.reserve(point_size + nonce_length + tag_length
                         + cipher_text.size());
  combined_bytes.resize(point_size + nonce_length + tag_length);
  CHECK_N_0(
    EC_POINT_point2oct(group, public_key, POINT_CONVERSION_UNCOMPRESSED,
                       reinterpret_cast<unsigned char*>(combined_bytes.data()),
                       point_size, bignum_ctx));
  std::memcpy(combined_bytes.data() + point_size, nonce.data(), nonce_length);
  std::memcpy(combined_bytes.data() + point_size + nonce_length, tag.data(),
              tag_length);
  combined_bytes += cipher_text;
  return detail::base64::encode(combined_bytes);
}

auto decrypt(std::string_view base64_ciphertext,
             const string_keypair& keypair) -> caf::expected<std::string> {
  // The platform is using a fairly "default" version of ECIES, ie. AES256-GCM
  // Mode with HKDF. as shared secret. Of course, using openssl this turns into
  // an insane amount of low-level code.
  // ---
  // ciphertext  =   ephemeral_key   | nonce (iv) | tag  | cipherdata
  // bytes                 65        |   16       |  16  |   ..rest
  // ---
  // Decode the input.
  auto pubkey_bytes = boost::algorithm::unhex(keypair.public_key);
  auto raw_ciphertext = detail::base64::decode(base64_ciphertext);
  auto ephemeral_key = std::array<unsigned char, 65>{};
  std::copy_n(raw_ciphertext.data(), 65, ephemeral_key.begin());
  auto iv = std::array<unsigned char, 16>{};
  std::copy_n(raw_ciphertext.data() + 65, 16, iv.begin());
  auto tag = std::array<unsigned char, 16>{};
  std::copy_n(raw_ciphertext.data() + 65 + 16, 16, tag.begin());
  auto cipher = std::vector<unsigned char>(
    raw_ciphertext.begin() + 65 + 16 + 16, raw_ciphertext.end());
  // Set up required objects.
  DECLARE(secret_number, BN_new(), BN_clear_free);
  DECLARE(group, EC_GROUP_new_by_curve_name(NID_secp256k1), EC_GROUP_free);
  DECLARE(bignum_ctx, BN_CTX_new(), BN_CTX_free);
  DECLARE(public_point, EC_POINT_new(group), EC_POINT_free);
  DECLARE(shared_point, EC_POINT_new(group), EC_POINT_free);
  DECLARE(cipher_ctx, EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
  // Initialize the points
  CHECK_N_0(BN_hex2bn(&secret_number, keypair.private_key.c_str()));
  CHECK_EQ_1(EC_POINT_oct2point(group, public_point, ephemeral_key.data(),
                                ephemeral_key.size(), bignum_ctx));
  CHECK_EQ_1(EC_POINT_is_on_curve(group, public_point, bignum_ctx));
  CHECK_EQ_1(EC_POINT_mul(group, shared_point, NULL, public_point,
                          secret_number, bignum_ctx));
  // Compute shared_secret = HKDF(public_point | shared_point)
  constexpr size_t POINT_SIZE = 65;
  auto combined_bytes = std::array<unsigned char, 2 * POINT_SIZE>{};
  CHECK_N_0(EC_POINT_point2oct(group, public_point,
                               POINT_CONVERSION_UNCOMPRESSED,
                               combined_bytes.data(), POINT_SIZE, bignum_ctx));
  CHECK_N_0(EC_POINT_point2oct(
    group, shared_point, POINT_CONVERSION_UNCOMPRESSED,
    combined_bytes.data() + POINT_SIZE, POINT_SIZE, bignum_ctx));
  auto bytes = std::string_view{reinterpret_cast<char*>(combined_bytes.begin()),
                                reinterpret_cast<char*>(combined_bytes.end())};
  TRY(auto shared_secret, hkdf(bytes));
  // ECIES implementation inspired by https://github.com/insanum/ecies
  auto plaintext = std::vector<unsigned char>(base64_ciphertext.size(), '\0');
  auto len = static_cast<int>(plaintext.size());
  // NB: Not sure why we need to set up encryption here, but decryption fails
  // without the call.
  CHECK_EQ_1(
    EVP_EncryptInit_ex2(cipher_ctx, EVP_aes_256_gcm(), NULL, NULL, NULL));
  CHECK_EQ_1(EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_IVLEN, 16, NULL));
  CHECK_EQ_1(EVP_DecryptInit_ex(cipher_ctx, NULL, NULL, shared_secret.data(),
                                iv.data()));
  CHECK_EQ_1(EVP_DecryptUpdate(cipher_ctx, plaintext.data(), &len,
                               cipher.data(), cipher.size()));
  auto total_len = len;
  CHECK_EQ_1(EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_TAG, tag.size(),
                                 tag.data()));
  fmt::print("{}\n",
             std::string{reinterpret_cast<const char*>(plaintext.data()),
                         plaintext.size()});
  // Finalize the decryption session. (this checks the tag matches)
  // TODO: is the +len correct here?
  CHECK_EQ_1(EVP_DecryptFinal_ex(cipher_ctx, (plaintext.data() + len), &len));
  total_len += len;
  plaintext.resize(total_len);
  // TODO: Probably makes more sense to return a
  // raw byte buffer here, and let the caller do
  // the check for valid utf8.
  return std::string{plaintext.begin(), plaintext.end()};
}

} // namespace tenzir::ecc
