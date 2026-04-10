# Domain Attestation

Validator domains can publish a lightweight attestation file so the dashboard can verify that a public website and validator key belong together.

## File location

Host this file over HTTPS at:

`https://<your-domain>/.well-known/postfiat.toml`

## Minimal file contents

```toml
[[VALIDATORS]]
public_key = "nHYourValidatorPublicKeyHere"
```

## Notes

- The readiness checker looks for a `[[VALIDATORS]]` entry containing your validator public key.
- The file can include more than one validator entry if needed.
- Keep the domain in your validator metadata aligned with the same hostname that serves this file.

## Basic hosting guidance

1. Create the directory `/.well-known/` on the web root for your validator domain.
2. Save `postfiat.toml` with the validator entry shown above.
3. Confirm the file is reachable in a browser at `https://<your-domain>/.well-known/postfiat.toml`.
4. Make sure your TLS certificate is valid and the domain resolves to the intended validator host or proxy.
