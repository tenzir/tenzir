#!/usr/bin/env ruby

require "base64"
require "json"
require "net/http"
require "openssl"
require "uri"

required_variables = %w[
  APPLE_API_ISSUER_ID
  APPLE_API_KEY_ID
  APPLE_API_KEY_P8_BASE64
]
missing_variables = required_variables.select { |variable| ENV[variable].to_s.empty? }
unless missing_variables.empty?
  abort "::error::Missing required variables: #{missing_variables.join(", ")}"
end

def base64url(value)
  Base64.urlsafe_encode64(value, padding: false)
end

def jwt_signature(key, message)
  sequence = OpenSSL::ASN1.decode(key.sign(OpenSSL::Digest.new("SHA256"), message))
  integers = sequence.value.map(&:value)
  unless integers.size == 2
    abort "::error::Unexpected ECDSA signature from Apple API key"
  end
  integers.map do |integer|
    [integer.to_s(16).rjust(64, "0")].pack("H*")
  end.join
end

key_id = ENV.fetch("APPLE_API_KEY_ID")
issuer_id = ENV.fetch("APPLE_API_ISSUER_ID")
private_key = OpenSSL::PKey.read(
  Base64.decode64(ENV.fetch("APPLE_API_KEY_P8_BASE64")),
)

now = Time.now.to_i
header = base64url(JSON.generate(alg: "ES256", kid: key_id, typ: "JWT"))
payload = base64url(
  JSON.generate(
    aud: "appstoreconnect-v1",
    exp: now + 10 * 60,
    iat: now,
    iss: issuer_id,
  ),
)
unsigned_token = "#{header}.#{payload}"
token = "#{unsigned_token}.#{base64url(jwt_signature(private_key, unsigned_token))}"

uri = URI("https://api.appstoreconnect.apple.com/v1/apps?limit=1")
request = Net::HTTP::Get.new(uri)
request["Authorization"] = "Bearer #{token}"
response = Net::HTTP.start(
  uri.hostname,
  uri.port,
  open_timeout: 10,
  read_timeout: 30,
  use_ssl: true,
) { |http| http.request(request) }

if response.is_a?(Net::HTTPSuccess)
  puts "The App Store Connect API accepted the current Apple agreements."
  exit 0
end

errors = begin
  Array(JSON.parse(response.body)["errors"])
rescue JSON::ParserError
  []
end
codes = errors.filter_map { |error| error["code"] }
details = errors.filter_map { |error| error["detail"] || error["title"] }.join(" ")

if codes.include?("FORBIDDEN.REQUIRED_AGREEMENTS_MISSING_OR_EXPIRED")
  abort "::error::A required Apple Developer agreement is missing or expired"
end

message = "App Store Connect API check failed with HTTP #{response.code}"
message += ": #{details}" unless details.empty?
abort "::error::#{message}"
