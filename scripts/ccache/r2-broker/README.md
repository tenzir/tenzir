# ccache R2 broker

This Cloudflare Worker exchanges a GitHub Actions OIDC token for scoped
Cloudflare R2 temporary credentials. The ccache S3 helper then uses the returned
S3-compatible session credentials directly against R2.

## Flow

1. The GitHub workflow starts the ccache S3 helper with
   `CCACHE_S3_AUTH=cloudflare-r2-oidc-broker`.
2. The helper requests a GitHub Actions OIDC token.
3. The helper sends that token to this Worker.
4. The Worker verifies the token issuer, audience, repository, and optional
   subject.
5. The Worker calls Cloudflare's R2 temporary credentials API.
6. The helper uses the returned access key, secret key, and session token for
   R2 S3 requests.
7. Botocore refreshes the temporary credentials through the Worker before they
   expire.

## Cloudflare Setup

### 1. Create or choose an R2 bucket

In the Cloudflare dashboard:

1. Open **R2 Object Storage**.
2. Create a bucket, or choose an existing bucket.
3. Record the bucket name.

Use this bucket name for both:

- GitHub variable `CCACHE_R2_BUCKET`
- GitHub variable `CCACHE_S3_BUCKET`

### 2. Record the Cloudflare account ID

In the Cloudflare dashboard, copy the account ID for the account that owns the
R2 bucket.

Use it as GitHub secret:

```text
TENZIR_CLOUDFLARE_ACCOUNT_ID=<cloudflare-account-id>
```

### 3. Create an R2 parent access key

The Worker needs a parent R2 access key ID. Cloudflare uses this parent key when
minting temporary credentials.

In the Cloudflare dashboard:

1. Open **R2 Object Storage**.
2. Open **Manage API tokens**.
3. Create an R2 token for the bucket.
4. Select **Object Read & Write** permissions.
5. Under bucket scope, select **Apply to specific buckets only** and choose the
   cache bucket.
6. Copy the **Access Key ID** from the S3 credentials section.

Use the access key ID as GitHub secret:

```text
CCACHE_R2_PARENT_ACCESS_KEY_ID=<r2-access-key-id>
```

The Worker does not need the parent R2 secret key for the API-based temporary
credentials flow.

### 4. Create a Cloudflare API token for the Worker

The Worker calls Cloudflare's R2 temporary credentials API. Create a Cloudflare
API token that can mint temporary credentials for the cache bucket.

In the Cloudflare dashboard:

1. Open **Manage Account** -> **Account API Tokens**.
2. Create a custom token.
3. Add this account permission:

   ```text
   Account -> Workers R2 Storage -> Write
   ```

4. Under account resources, select **Include** and choose the account that owns
   the R2 bucket.
5. Copy the token value.

Use it as GitHub secret:

```text
CCACHE_R2_API_TOKEN=<cloudflare-api-token-for-r2-temp-credentials>
```

The temporary credentials themselves are restricted by the Worker request body:
`R2_BUCKET`, `R2_ALLOWED_PREFIXES`, and
`R2_TEMP_CREDENTIAL_PERMISSION=object-read-write`. Cloudflare does not expose a
bucket selector for this account API token in the same way the R2 S3 token UI
does, so keep this token dedicated to the broker.

### 5. Create a Cloudflare API token for deployment

The GitHub workflow deploys the Worker with Wrangler.

Create a separate Cloudflare API token with permissions to deploy Workers in the
same Cloudflare account.

Use it as GitHub secret:

```text
CLOUDFLARE_WORKERS_API_TOKEN=<cloudflare-api-token-for-worker-deploys>
```

Keep this separate from `CCACHE_R2_API_TOKEN`: the deployment token should not
need R2 data permissions, and the broker token should not need Worker deployment
permissions.

## GitHub Configuration

### Required repository variables

```text
CCACHE_R2_BROKER_ENABLED=true
CCACHE_R2_BUCKET=<r2-bucket-name>
CCACHE_S3_BUCKET=<r2-bucket-name>
CCACHE_S3_AUTH=cloudflare-r2-oidc-broker
CCACHE_R2_OIDC_BROKER_URL=https://tenzir-ccache-r2-broker.<workers-subdomain>.workers.dev
```

### Required repository secrets

```text
CLOUDFLARE_WORKERS_API_TOKEN=<cloudflare-api-token-for-worker-deploys>
CCACHE_R2_API_TOKEN=<cloudflare-api-token-for-r2-temp-credentials>
CCACHE_R2_PARENT_ACCESS_KEY_ID=<r2-access-key-id>
TENZIR_CLOUDFLARE_ACCOUNT_ID=<cloudflare-account-id>
```

### Optional repository variables

```text
CCACHE_R2_OIDC_AUDIENCE=ccache-r2-broker
CCACHE_R2_GITHUB_SUBJECT=<exact-github-oidc-subject>
CCACHE_R2_ALLOWED_PREFIXES=tenzir/
CCACHE_R2_TEMP_CREDENTIAL_PERMISSION=object-read-write
CCACHE_R2_TEMP_CREDENTIAL_TTL_SECONDS=3600
CCACHE_S3_PREFIX=tenzir
```

`CCACHE_R2_GITHUB_SUBJECT` is stricter than repository matching. Leave it unset
until you know the exact `sub` claim emitted by the workflows that should use
the broker.

`CCACHE_R2_ALLOWED_PREFIXES` should match the cache prefix when possible. For
example, if `CCACHE_S3_PREFIX=tenzir`, set:

```text
CCACHE_R2_ALLOWED_PREFIXES=tenzir/
```

## Deploy

After the variables and secrets are set, run the **ccache R2 broker** workflow
manually once, or push a change under `scripts/ccache/r2-broker/`.

The workflow deploys the Worker and stores these Worker secrets:

- `CLOUDFLARE_API_TOKEN`
- `R2_PARENT_ACCESS_KEY_ID`

The Worker URL is usually:

```text
https://tenzir-ccache-r2-broker.<workers-subdomain>.workers.dev
```

Use that URL as `CCACHE_R2_OIDC_BROKER_URL`.
