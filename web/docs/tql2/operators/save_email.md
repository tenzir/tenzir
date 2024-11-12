# save_email

Saves bytes through an SMTP server.

```tql
save_email recipient:str, [endpoint:str, from=str, subject=str, username=str,
      password=str, authzid=str, authorization=str,
      skip_peer_verification=bool, skip_hostname_verification=bool,
      mime=bool, verbose=bool]
```

## Description

The `save_email` operator establishes a SMTP(S) connection to a mail server and sends
bytes as email body.

### `recipient: str`

The recipient of the mail.

The expected format is either `Name <user@example.org>` with the email in angle
brackets, or a plain email adress, such as `user@example.org`.

### `endpoint = str (optional)`

The endpoint of the mail server.

To choose between SMTP and SMTPS, provide a URL with with the corresponding
scheme. For example, `smtp://127.0.0.1:25` will establish an unencrypted
connection, whereas `smtps://127.0.0.1:25` an encrypted one. If you specify a
server without a schema, the protocol defaults to SMTPS.

Defaults to `smtp://localhost:25`.

### `from = str (optional)`

The `From` header.

If you do not specify this parameter, an empty address is sent to the SMTP
server which might cause the email to be rejected.

### `subject = str (optional)`

The `Subject` header.

### `username = str (optional)`

The username in an authenticated SMTP connection.

### `password = str (optional)`

The password in an authenticated SMTP connection.

### `authzid = str (optional)`

The authorization identity in an authenticated SMTP connection.

This option is only applicable to the PLAIN SASL authentication mechanism where
it is optional. When not specified only the authentication identity (`authcid`)
as specified by the username is sent to the server, along with the password. The
server derives an `authzid` from the `authcid` when not provided, which it then
uses internally. When the `authzid` is specified it can be used to access
another user's inbox, that the user has been granted access to, or a shared
mailbox.

### `authorization = str (optional)`

The authorization options for an authenticated SMTP connection.

This login option defines the preferred authentication mechanism, e.g.,
`AUTH=PLAIN`, `AUTH=LOGIN`, or `AUTH=*`.

### `skip_peer_verification = bool (optional)`

If to skip certificate verification.

By default, an SMTPS connection verifies the authenticity of the peer's
certificate. During connection negotiation, the server sends a certificate
indicating its identity. We verify whether the certificate is authentic,
i.e., that you can trust that the server is who the certificate says it is.

If `true`, disables loading of the CA certificates and verification of
the peer certificate.

### `skip_hostname_verification = bool (optional)`

If to ignore verification of the server name in the certificate.

When negotiating TLS and SSL connections, the server sends a certificate
indicating its identity. By default, that certificate must indicate that the
server is the server to which you meant to connect, or the connection fails.
That is, the server has to have the same name in the certificate as is in the
URL you operate against. We consider the server the intended one when the
*Common Name* field or a *Subject Alternate Name* field in the certificate
matches the hostname in the URL.

Setting this to `true` skips this check, but it makes the connection insecure.

Defaults to `false`.

### `mime = bool (optional)`

If to wrap the chunk into a MIME part.

The operator uses the metadata of the byte chunk for the
`Content-Type` MIME header.

Defaults to `false`.

### `verbose = bool (optional)`

Enables verbose output on stderr.

This option is helpful for debugging on the command line.

Defaults to `false`.

## Examples

Send the Tenzir version string as CSV to `user@example.org`:

```tql
version
write_csv
save_email "user@example.org"
```

Send the email body as MIME part:

```tql
version
write_json
save_email "user@example.org", mime=true
```

This may result in the following email body:

```
--------------------------s89ecto6c12ILX7893YOEf
Content-Type: application/json
Content-Transfer-Encoding: quoted-printable

{
  "version": "4.10.4+ge0a060567b-dirty",
  "build": "ge0a060567b-dirty",
  "major": 4,
  "minor": 10,
  "patch": 4
}

--------------------------s89ecto6c12ILX7893YOEf--
```
