# shared-security

> **Role in the platform:** Reusable security library — provides the in-process PASETO token verification primitives, PII masking utilities, and the pluggable `PasetoVerifier` interface consumed by `ingestion-service` and `insights-query-service`.

---

## Table of Contents

- [Purpose](#purpose)
- [Package Structure](#package-structure)
- [PASETO Implementation](#paseto-implementation)
  - [Why PASETO over JWT](#why-paseto-over-jwt)
  - [PasetoVerifier Interface](#pasetoverifier-interface)
  - [v4.public — Ed25519 (PasetoV4PublicVerifier)](#v4public--ed25519-paseto-v4publicverifier)
  - [v4.local — XChaCha20 + BLAKE2b (PasetoV4LocalVerifier)](#v4local--xchacha20--blake2b-paseto-v4localverifier)
  - [PasetoV4Local — Encryption Primitives](#paseto-v4local--encryption-primitives)
  - [PasetoV4LocalIssuer — Token Minting](#paseto-v4localissuer--token-minting)
  - [PasetoClaims](#pasetoclaims)
  - [ClaimsValidator](#claimsvalidator)
- [PII Utilities](#pii-utilities)
  - [PiiMasker](#piimasker)
- [Cryptographic Internals](#cryptographic-internals)
  - [PAE (Pre-Authentication Encoding)](#pae-pre-authentication-encoding)
  - [XChaCha20](#xchacha20)
  - [BLAKE2b](#blake2b)
  - [Ed25519Keys](#ed25519keys)
  - [SymmetricKeys](#symmetrickeys)
- [Token Exchange Flow (Option C)](#token-exchange-flow-option-c)
- [Adding to Your Module](#adding-to-your-module)
- [Building & Testing](#building--testing)
- [Security Notes](#security-notes)

---

## Purpose

`shared-security` eliminates the most common sources of authentication bugs by:

1. **Centralizing all PASETO cryptography** in one auditable library — services cannot implement their own token parsing.
2. **Providing a pluggable `PasetoVerifier` interface** so the filter in each service is agnostic to whether it is verifying an external `v4.public` token or a gateway-minted internal `v4.local` token.
3. **Sharing `ClaimsValidator`** so `exp`/`nbf`/`iss`/`aud` checks are identical across both token flavours — they cannot drift independently.
4. **Providing `PiiMasker`** so all services can safely log user-facing data without leaking emails or IP addresses.

---

## Package Structure

```
com.java.security/
│
├── paseto/
│   ├── PasetoVerifier.java           # Pluggable verification interface: verify(token) → PasetoClaims
│   ├── PasetoClaims.java             # Verified claims record: tenantId, scopes, allowedCampaigns, iss, aud, exp, nbf, jti
│   ├── PasetoException.java          # Runtime exception thrown on any verification failure
│   │
│   ├── PasetoV4PublicVerifier.java   # Implements PasetoVerifier for v4.public (Ed25519 signature)
│   ├── PasetoV4LocalVerifier.java    # Implements PasetoVerifier for v4.local (symmetric decrypt + auth)
│   ├── PasetoV4Local.java            # Low-level v4.local encrypt/decrypt (XChaCha20 + BLAKE2b)
│   ├── PasetoV4LocalIssuer.java      # Mints v4.local tokens (edge / token-exchange step)
│   │
│   ├── ClaimsValidator.java          # Shared exp/nbf/iss/aud validation (package-private)
│   ├── Ed25519Keys.java              # Parses Ed25519 public keys (PEM / X.509 DER / raw hex|base64)
│   ├── SymmetricKeys.java            # Parses 32-byte symmetric keys (hex / base64)
│   ├── Pae.java                      # Pre-Authentication Encoding (PAE) per PASETO spec
│   ├── XChaCha20.java                # XChaCha20 stream cipher (HChaCha20 + BouncyCastle ChaCha20)
│   └── Blake2b.java                  # Keyed BLAKE2b-256 MAC (BouncyCastle)
│
└── pii/
    └── PiiMasker.java                # Masks emails + IPv4 addresses; SHA-256 pseudonymizer
```

---

## PASETO Implementation

### Why PASETO over JWT

| Vulnerability class | Legacy JWT | PASETO `v4.public` |
|:--------------------|:-----------|:-------------------|
| **Algorithm confusion** (`RS256→HS256`) | Attacker can swap the `alg` header | Impossible — version string locks the algorithm |
| **`alg: none`** | Accepted by many parsers | Impossible — signatures are mandatory |
| **Weak crypto agility** | SHA-1, short keys via config | Opinionated: Ed25519 (v4.public), XChaCha20+BLAKE2b (v4.local) |
| **Parse-before-verify** | Arbitrary JSON header parsed first | `v4.public.` prefix checked before any work |

### PasetoVerifier Interface

```java
public interface PasetoVerifier {
    /**
     * Verify the token and return its validated claims.
     *
     * @throws PasetoException on malformed token, signature/MAC failure, or exp/nbf/iss/aud mismatch
     */
    PasetoClaims verify(String token);
}
```

The Spring `PasetoSecurityConfig` in each service builds the correct implementation at startup based on `platform.security.paseto.mode`:

```java
if ("local".equalsIgnoreCase(mode)) {
    return new PasetoV4LocalVerifier(localKey, issuer, audience, clockSkew);
}
return new PasetoV4PublicVerifier(publicKey, issuer, audience, clockSkew);
```

---

### v4.public — Ed25519 (`PasetoV4PublicVerifier`)

Verifies externally-signed `v4.public` tokens (used when `mode=public`).

**Verification steps (PASETO spec §v4.public):**

1. Assert the token starts with `v4.public.`
2. Base64url-decode the payload into `message ‖ signature(64 bytes)`
3. Recompute `m2 = PAE("v4.public.", message, footer, implicit)`
4. Verify the Ed25519 signature of `m2` with the Ed25519 public key
5. Parse JSON claims; validate `exp`/`nbf`/`iss`/`aud` via `ClaimsValidator`

```java
PasetoVerifier verifier = new PasetoV4PublicVerifier(
    publicKeyBase64,    // X.509 DER encoded Ed25519 public key (base64)
    "auth.platform.internal",  // expected iss (null = not enforced)
    "media-pulse-iq",          // expected aud (null = not enforced)
    Duration.ofSeconds(30)     // clock skew tolerance
);

PasetoClaims claims = verifier.verify(token);
// → throws PasetoException if signature invalid, expired, or iss/aud mismatch
```

The implementation uses the **JDK 21 native `Ed25519` provider** — no BouncyCastle dependency for this path.

---

### v4.local — XChaCha20 + BLAKE2b (`PasetoV4LocalVerifier`)

Verifies gateway-minted `v4.local` tokens (used when `mode=local`, the deployed default — Option C).

**Decryption steps (PASETO spec §v4.local):**

1. Assert the token starts with `v4.local.`
2. Base64url-decode body into `nonce(32) ‖ ciphertext ‖ tag(32)`
3. Derive encryption subkey `Ek ‖ n2 = BLAKE2b(key, INFO_ENCRYPTION_KEY ‖ nonce, 56 bytes)`
4. Derive authentication key `ak = BLAKE2b(key, INFO_AUTH_KEY ‖ nonce, 32 bytes)`
5. Verify `tag == BLAKE2b(ak, PAE(header, nonce, ciphertext, footer, implicit), 32 bytes)` — **constant-time compare**
6. Decrypt `message = XChaCha20(Ek, n2, ciphertext)`
7. Parse JSON claims; validate via `ClaimsValidator`

```java
PasetoVerifier verifier = new PasetoV4LocalVerifier(
    keyBase64,          // 32-byte symmetric key (hex or base64)
    "edge",             // expected iss
    "media-pulse-iq",   // expected aud
    Duration.ofSeconds(30)
);

PasetoClaims claims = verifier.verify(internalToken);
```

Both `PasetoV4LocalVerifier` and `PasetoV4LocalVerifier.verify(token, implicitAssertion)` accept an optional implicit assertion for channel/route binding (roadmap feature).

---

### PasetoV4Local — Encryption Primitives

Low-level static utility for encrypting and decrypting `v4.local` token bodies. Used by `PasetoV4LocalIssuer` (mint) and `PasetoV4LocalVerifier` (verify).

```java
// Encrypt raw JSON claims bytes into a v4.local token
String token = PasetoV4Local.encrypt(key32bytes, claimsJson.getBytes(UTF_8));

// Decrypt and verify; returns plaintext JSON bytes
byte[] plaintext = PasetoV4Local.decrypt(key32bytes, token);

// With footer and implicit assertion (optional)
String token = PasetoV4Local.encrypt(key, message, footer, implicitAssertion);
byte[] plain  = PasetoV4Local.decrypt(key, token, implicitAssertion);
```

Authentication tag mismatch (wrong key, tampered ciphertext) → `PasetoException("Invalid token authentication tag")`.

---

### PasetoV4LocalIssuer — Token Minting

Produces short-lived `v4.local` tokens. In production, this logic runs **at the Kong edge gateway** (as a Kong plugin or a thin token-exchange microservice) after the external `v4.public` token has been verified. The class is provided here so the platform can mint tokens in tests and the gateway can share the same library.

```java
PasetoV4LocalIssuer issuer = new PasetoV4LocalIssuer(
    sharedKeyBase64,
    "edge",           // iss
    "media-pulse-iq"  // aud
);

// Mint a 60-second internal token for the verified tenant
String internalToken = issuer.issue(
    "walmart_us",                     // tenantId (from verified external token)
    List.of("read:ads"),              // scopes
    List.of("cmp_spring_99a"),        // allowed_campaigns
    Duration.ofSeconds(60)            // tiny TTL — internal tokens are ephemeral
);
```

The issuer does **not** mint `jti` by default. A `kid` footer can be added in a future rotation scheme.

---

### PasetoClaims

Immutable record holding all verified claims extracted from a token.

```java
public record PasetoClaims(
    String       tenantId,         // tenant_id claim
    List<String> scopes,           // scopes claim (e.g. ["read:ads", "write:events"])
    List<String> allowedCampaigns, // allowed_campaigns claim (null/empty = no restriction)
    String       issuer,           // iss
    String       audience,         // aud
    String       subject,          // sub
    String       expiration,       // exp (ISO-8601)
    String       notBefore,        // nbf (ISO-8601)
    String       issuedAt,         // iat (ISO-8601)
    String       tokenId           // jti (for future revocation denylist)
) {
    boolean hasScope(String scope);              // e.g. hasScope("read:ads")
    boolean canAccessCampaign(String campaignId); // false only if allowedCampaigns is non-empty and doesn't contain campaignId
    Instant expirationInstant();
    Instant notBeforeInstant();
}
```

**Authorization helpers:**

```java
// Scope check — used by PasetoAuthenticationFilter
if (!claims.hasScope("read:ads")) { return 403; }

// Campaign allow-list — used by InsightsServiceImpl
if (!claims.canAccessCampaign(campaignId)) { return 403; }
```

`canAccessCampaign` returns `true` when `allowedCampaigns` is `null` or empty (no restriction configured on the token).

---

### ClaimsValidator

Package-private shared validator — ensures `exp`/`nbf`/`iss`/`aud` checks are **identical** across `PasetoV4PublicVerifier` and `PasetoV4LocalVerifier`. The two verifiers call `ClaimsValidator.validate(claims, expectedIssuer, expectedAudience, clockSkew)` after parsing claims JSON.

```
Token expired?      → "Token expired"
Token not yet valid? → "Token not yet valid"
Wrong issuer?       → "Unexpected token issuer"
Wrong audience?     → "Unexpected token audience"
```

If `expectedIssuer`/`expectedAudience` is blank (or configured as empty string), the corresponding check is skipped.

---

## PII Utilities

### PiiMasker

Keeps personally identifiable information out of logs and low-trust sinks (OWASP A09).

```java
// Mask emails and IPv4 addresses in log lines
String safe = PiiMasker.mask("User john@example.com from 192.168.1.1");
// → "User ***@*** from ***.***.***.***"

// One-way pseudonym for correlation without exposing the raw ID
String anon = PiiMasker.pseudonymize("usr_abc123");
// → "anon_5f4dcc3b5aa7" (first 12 hex chars of SHA-256)
```

Use `pseudonymize` for user/session identifiers in DLQ messages and structured log fields where you need to correlate events without storing the raw ID.

---

## Cryptographic Internals

### PAE (Pre-Authentication Encoding)

`Pae.encode(byte[]... pieces)` implements the PASETO Pre-Authentication Encoding:

```
LE64(n) ‖ LE64(len(p1)) ‖ p1 ‖ LE64(len(p2)) ‖ p2 ‖ …
```

Where `LE64` is an 8-byte little-endian encoding with the high bit of the last byte cleared. PAE prevents length-extension and canonicalization attacks by binding all authenticated pieces to their lengths before any MAC or signature is computed.

### XChaCha20

`XChaCha20.process(key32, nonce24, input)` — stream cipher used in `v4.local` encryption/decryption.

Implementation:
1. Derives a subkey from the first 16 bytes of the 24-byte nonce using **HChaCha20** (implemented natively without BouncyCastle).
2. Passes the subkey + last 8 bytes of nonce (zero-padded to 12 bytes with a counter of 0) to BouncyCastle's `ChaCha20` engine for the actual stream cipher operation.

This is the standard XChaCha20 construction: HChaCha20 extends the nonce from 12 to 24 bytes, enabling random nonce generation without nonce-reuse risk.

### BLAKE2b

`Blake2b.mac(key, message, outputLength)` — keyed BLAKE2b-256 used for key derivation and authentication in `v4.local`.

Calls `BouncyCastle.Blake2bDigest` in keyed mode. Two distinct operations in `PasetoV4Local`:

| Call | Key | Message | Output | Purpose |
|:-----|:----|:--------|:-------|:--------|
| Key derivation | `localKey` | `INFO_ENCRYPTION_KEY ‖ nonce` | 56 bytes | Produces `Ek` (32B) + `n2` (24B) for XChaCha20 |
| Auth key | `localKey` | `INFO_AUTH_KEY ‖ nonce` | 32 bytes | `ak` for the final MAC tag |
| MAC | `ak` | `PAE(header, nonce, ct, footer, implicit)` | 32 bytes | Authentication tag |

### Ed25519Keys

`Ed25519Keys.parsePublicKey(material)` — accepts the public key in any of three formats:

| Format | Example |
|:-------|:--------|
| X.509 DER base64 | `MCowBQYDK2VwAyEA...` (standard Java `KeyPairGenerator` output) |
| Raw 32-byte hex | `5f4dcc3b5aa765d61d83...` (64 hex chars) |
| Raw 32-byte base64 | `X03MO1qnZobpcBqPlmDW...` (44 chars) |

### SymmetricKeys

`SymmetricKeys.parse32(material)` — parses a 32-byte symmetric key:

| Format | Description |
|:-------|:------------|
| Hex string | 64 hexadecimal characters |
| Base64 string | Standard or URL-safe base64, padded or unpadded |

Throws `PasetoException("v4.local key must be exactly 32 bytes")` if the decoded length is not exactly 32.

---

## Token Exchange Flow (Option C)

This library implements both sides of the token-exchange (Option C) design:

```
External token (v4.public, Ed25519)
          │
          │ Kong verifies with Ed25519 public key
          ▼
PasetoV4LocalIssuer.issue(tenantId, scopes, allowedCampaigns, ttl=60s)
          │
          │ Mints internal v4.local token (shared symmetric key)
          ▼
X-Internal-Token header forwarded to microservice
          │
          │ PasetoV4LocalVerifier.verify(token)
          ▼
PasetoClaims (tenantId, scopes, allowedCampaigns, exp, iss, aud)
```

The external token **never** enters the internal mesh. The internal token has a tiny TTL (~60 seconds), an audience scoped to `media-pulse-iq`, and carries the same claims as the external token. Services verify the internal token with the shared `PASETO_LOCAL_KEY`.

---

## Adding to Your Module

```xml
<dependency>
    <groupId>com.java</groupId>
    <artifactId>shared-security</artifactId>
</dependency>
```

Transitive dependencies provided:
- `jackson-databind` (JSON claims parsing)
- `bcprov-jdk18on` (BouncyCastle: XChaCha20 + BLAKE2b for v4.local)

---

## Building & Testing

```bash
# Build and install
mvn -pl shared-security install

# Run tests
mvn -pl shared-security test
```

### Test Coverage

| Test Class | Tests | Covers |
|:-----------|:-----:|:-------|
| `PasetoV4PublicVerifierTest` | 5 | Round-trip, tampered payload, expired, wrong key, wrong issuer |
| `PasetoV4LocalVerifierTest` | 6 | Round-trip, raw payload, tampered, expired, wrong key, wrong issuer |
| `PiiMaskerTest` | — | Email masking, IPv4 masking, pseudonymization |

All tests are pure unit tests — no external dependencies required.

---

## Security Notes

| Concern | Mitigation |
|:--------|:-----------|
| **Algorithm confusion** | `PasetoV4PublicVerifier` hard-rejects any token not starting with `v4.public.`; `PasetoV4LocalVerifier` hard-rejects anything not starting with `v4.local.` |
| **Timing side-channels** | `PasetoV4Local.decrypt` uses `MessageDigest.isEqual` (constant-time byte comparison) for MAC verification |
| **Key material in logs** | `SymmetricKeys` and `Ed25519Keys` never log the raw key; `PiiMasker` is available for any string that might contain sensitive data |
| **Null-safe claims** | `PasetoClaims.canAccessCampaign` and `hasScope` handle null/empty lists safely — the filter never throws NPE on malformed token payloads |
| **Fail-closed** | `SymmetricKeys.parse32` throws if the decoded key is not exactly 32 bytes — prevents silent truncation or padding accepting a weak key |
| **Clock skew** | `ClaimsValidator` applies a configurable skew (default 60s, prod 30s) to `exp` and `nbf` checks to tolerate minor clock drift between services |
| **BouncyCastle scope** | BouncyCastle is used only for `XChaCha20` and `BLAKE2b` — primitives not available in the JDK 21 standard library. Ed25519 verification uses the JDK 21 native `Ed25519` provider |

