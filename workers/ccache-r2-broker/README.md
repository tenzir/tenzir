# ccache R2 broker

This Cloudflare Worker exchanges a GitHub Actions OIDC token for scoped
Cloudflare R2 temporary credentials. The ccache S3 helper can then use the
returned credentials against R2's S3-compatible API.

Required Worker variables:

- `CLOUDFLARE_ACCOUNT_ID`
- `R2_BUCKET`
- `R2_PARENT_ACCESS_KEY_ID`
- `GITHUB_REPOSITORY`, for example `tenzir/tenzir`

Required Worker secret:

- `CLOUDFLARE_API_TOKEN`
- `R2_PARENT_ACCESS_KEY_ID`

Optional Worker variables:

- `GITHUB_SUBJECT`
- `GITHUB_OIDC_AUDIENCE`, defaults to `ccache-r2-broker`
- `R2_ALLOWED_PREFIXES`, comma-separated
- `R2_TEMP_CREDENTIAL_PERMISSION`, defaults to `object-read-write`
- `R2_TEMP_CREDENTIAL_TTL_SECONDS`, defaults to `3600`

Configure the ccache helper in GitHub Actions with:

```sh
CCACHE_S3_AUTH=cloudflare-r2-oidc-broker
CCACHE_R2_OIDC_BROKER_URL=https://<worker>.<subdomain>.workers.dev
CCACHE_R2_OIDC_AUDIENCE=ccache-r2-broker
CCACHE_S3_BUCKET=<r2-bucket>
```
