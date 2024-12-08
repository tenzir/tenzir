# Email

Tenzir supports sending events as email using the
[`save_email`](../../tql2/operators/save_email.md) operator. To this end, the
operator establishes a connection with an SMTP server that sends the message on
behalf of Tenzir.

![Pipeline to email](email.svg)

## Examples

### Email the Tenzir version as CSV message

```tql
version
write_csv
save_email "Example User <user@example.org>"
```

### Send the email body as MIME part

```tql
version
write_json
save_email "user@example.org, mime=true
```

This results in an email body of this shape:

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
