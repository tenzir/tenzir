const GITHUB_OIDC_ISSUER = "https://token.actions.githubusercontent.com";
const GITHUB_OIDC_JWKS_URL = `${GITHUB_OIDC_ISSUER}/.well-known/jwks`;

export default {
  async fetch(request, env) {
    try {
      if (request.method === "GET") {
        return jsonResponse({ ok: true });
      }
      if (request.method !== "POST") {
        return jsonResponse({ error: "method not allowed" }, 405);
      }
      const body = await request.json();
      const token = requireString(body.token, "token");
      const claims = await verifyGithubOidcToken(token, env);
      const credentials = await createTemporaryCredentials(env);
      return jsonResponse({
        accessKeyId: credentials.accessKeyId,
        secretAccessKey: credentials.secretAccessKey,
        sessionToken: credentials.sessionToken,
        bucket: env.R2_BUCKET,
        endpointUrl: `https://${env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        region: "auto",
        expiresAt: credentials.expiresAt ?? null,
        repository: claims.repository,
        subject: claims.sub,
      });
    } catch (error) {
      const status = error.status ?? 500;
      return jsonResponse({ error: error.message ?? "internal error" }, status);
    }
  },
};

async function verifyGithubOidcToken(token, env) {
  const [encodedHeader, encodedPayload, encodedSignature] = token.split(".");
  if (!encodedHeader || !encodedPayload || !encodedSignature) {
    throw httpError(401, "invalid OIDC token");
  }
  const header = JSON.parse(decodeBase64UrlText(encodedHeader));
  if (header.alg !== "RS256" || typeof header.kid !== "string") {
    throw httpError(401, "unsupported OIDC token header");
  }
  const payload = JSON.parse(decodeBase64UrlText(encodedPayload));
  const now = Math.floor(Date.now() / 1000);
  const expectedAudience = env.GITHUB_OIDC_AUDIENCE || "ccache-r2-broker";
  assertClaim(payload.iss === GITHUB_OIDC_ISSUER, "invalid OIDC issuer");
  assertClaim(audienceMatches(payload.aud, expectedAudience), "invalid OIDC audience");
  assertClaim(typeof payload.exp === "number" && payload.exp > now, "expired OIDC token");
  assertClaim(typeof payload.nbf !== "number" || payload.nbf <= now, "OIDC token not yet valid");
  if (env.GITHUB_REPOSITORY) {
    assertClaim(payload.repository === env.GITHUB_REPOSITORY, "invalid GitHub repository");
  }
  if (env.GITHUB_SUBJECT) {
    assertClaim(payload.sub === env.GITHUB_SUBJECT, "invalid GitHub subject");
  }
  const jwk = await findGithubJwk(header.kid);
  const key = await crypto.subtle.importKey(
    "jwk",
    jwk,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["verify"],
  );
  const verified = await crypto.subtle.verify(
    "RSASSA-PKCS1-v1_5",
    key,
    decodeBase64Url(encodedSignature),
    new TextEncoder().encode(`${encodedHeader}.${encodedPayload}`),
  );
  assertClaim(verified, "invalid OIDC token signature");
  return payload;
}

async function createTemporaryCredentials(env) {
  const accountId = requireEnv(env, "CLOUDFLARE_ACCOUNT_ID");
  const apiToken = requireEnv(env, "CLOUDFLARE_API_TOKEN");
  const bucket = requireEnv(env, "R2_BUCKET");
  const parentAccessKeyId = requireEnv(env, "R2_PARENT_ACCESS_KEY_ID");
  const ttlSeconds = Number(env.R2_TEMP_CREDENTIAL_TTL_SECONDS || "3600");
  const requestBody = {
    bucket,
    parentAccessKeyId,
    permission: env.R2_TEMP_CREDENTIAL_PERMISSION || "object-read-write",
    ttlSeconds,
  };
  const prefixes = splitCsv(env.R2_ALLOWED_PREFIXES);
  if (prefixes.length > 0) {
    requestBody.prefixes = prefixes;
  }
  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/temp-access-credentials`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestBody),
    },
  );
  const data = await response.json();
  if (!response.ok || !data.success) {
    throw httpError(response.status, "could not create R2 temporary credentials");
  }
  return data.result;
}

async function findGithubJwk(kid) {
  const response = await fetch(GITHUB_OIDC_JWKS_URL, {
    cf: { cacheTtl: 300, cacheEverything: true },
  });
  if (!response.ok) {
    throw httpError(503, "could not fetch GitHub OIDC keys");
  }
  const jwks = await response.json();
  const jwk = jwks.keys?.find((key) => key.kid === kid);
  if (!jwk) {
    throw httpError(401, "unknown OIDC signing key");
  }
  return jwk;
}

function audienceMatches(actual, expected) {
  if (Array.isArray(actual)) {
    return actual.includes(expected);
  }
  return actual === expected;
}

function assertClaim(condition, message) {
  if (!condition) {
    throw httpError(401, message);
  }
}

function requireEnv(env, name) {
  const value = env[name];
  if (typeof value !== "string" || value.length === 0) {
    throw httpError(500, `missing Worker setting ${name}`);
  }
  return value;
}

function requireString(value, name) {
  if (typeof value !== "string" || value.length === 0) {
    throw httpError(400, `${name} must be a non-empty string`);
  }
  return value;
}

function splitCsv(value) {
  if (!value) {
    return [];
  }
  return value
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function decodeBase64UrlText(value) {
  return new TextDecoder().decode(decodeBase64Url(value));
}

function decodeBase64Url(value) {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized.padEnd(normalized.length + ((4 - (normalized.length % 4)) % 4), "=");
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

function httpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
