# Web configuration

Exporters and services instrumented with the Exporter Toolkit share the same
web configuration file format. This is *experimental* and might change in the
future.

To specify which web configuration file to load, use the `--web.config.file` flag.

The file is written in [YAML format](https://en.wikipedia.org/wiki/YAML),
defined by the scheme described below.
Brackets indicate that a parameter is optional. For non-list parameters the
value is set to the specified default.

The file is read upon every http request, such as any change in the
configuration, so the certificates are picked up immediately.

Generic placeholders are defined as follows:

* `<boolean>`: a boolean that can take the values `true` or `false`
* `<filename>`: a valid path in the current working directory
* `<secret>`: a regular string that is a secret, such as a password
* `<string>`: a regular string
* `<int>`: a regular integer

```
tls_server_config:
  # Certificate for server to use to authenticate to client.
  # Expected to be passed as a PEM encoded sequence of bytes as a string.
  #
  # NOTE: If passing the cert inline, cert_file should not be specified below.
  [ cert: <string> ]

  # Key for server to use to authenticate to client.
  # Expected to be passed as a PEM encoded sequence of bytes as a string.
  #
  # NOTE: If passing the key inline, key_file should not be specified below.
  [ key: <secret> ]

  # CA certificate for client certificate authentication to the server.
  # Expected to be passed as a PEM encoded sequence of bytes as a string.
  #
  # NOTE: If passing the client_ca inline, client_ca_file should not be specified below.
  [ client_ca: <string> ]

  # Certificate and key files for server to use to authenticate to client.
  cert_file: <filename>
  key_file: <filename>

  # Server policy for client authentication. Maps to ClientAuth Policies.
  # For more detail on clientAuth options:
  # https://golang.org/pkg/crypto/tls/#ClientAuthType
  #
  # NOTE: If you want to enable client authentication, you need to use
  # RequireAndVerifyClientCert. Other values are insecure.
  [ client_auth_type: <string> | default = "NoClientCert" ]

  # CA certificate for client certificate authentication to the server.
  [ client_ca_file: <filename> ]

  # Verify that the client certificate has a Subject Alternate Name (SAN)
  # which is an exact match to an entry in this list, else terminate the
  # connection. SAN match can be one or multiple of the following: DNS,
  # IP, e-mail, or URI address from https://pkg.go.dev/crypto/x509#Certificate.
  [ client_allowed_sans:
    [ - <string> ] ]

  # Minimum TLS version that is acceptable.
  [ min_version: <string> | default = "TLS12" ]

  # Maximum TLS version that is acceptable.
  [ max_version: <string> | default = "TLS13" ]

  # List of supported cipher suites for TLS versions up to TLS 1.2. If empty,
  # Go default cipher suites are used. Available cipher suites are documented
  # in the go documentation:
  # https://golang.org/pkg/crypto/tls/#pkg-constants
  #
  # Note that only the cipher returned by the following function are supported:
  # https://pkg.go.dev/crypto/tls#CipherSuites
  [ cipher_suites:
    [ - <string> ] ]

  # Deprecated: this setting is accepted and ignored.
  #
  # It used to control whether the server followed its own cipher suite
  # preference, as expressed in the order of elements in cipher_suites, or the
  # client's. Go has selected the cipher suite itself since Go 1.17, taking
  # into account inferred client hardware, server hardware and security, and
  # the underlying setting has had no effect since then.
  [ prefer_server_cipher_suites: <bool> | default = true ]

  # Elliptic curves that will be used in an ECDHE handshake, in preference
  # order. Available curves are documented in the go documentation:
  # https://golang.org/pkg/crypto/tls/#CurveID
  [ curve_preferences:
    [ - <string> ] ]

http_server_config:
  # Enable HTTP/2 support. Note that HTTP/2 is only supported with TLS.
  # This can not be changed on the fly.
  [ http2: <boolean> | default = true ]
  # List of headers that can be added to HTTP responses.
  [ headers:
    # Set the Content-Security-Policy header to HTTP responses.
    # Unset if blank.
    [ Content-Security-Policy: <string> ]
    # Set the X-Frame-Options header to HTTP responses.
    # Unset if blank. Accepted values are deny and sameorigin.
    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Frame-Options
    [ X-Frame-Options: <string> ]
    # Set the X-Content-Type-Options header to HTTP responses.
    # Unset if blank. Accepted value is nosniff.
    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Content-Type-Options
    [ X-Content-Type-Options: <string> ]
    # Set the X-XSS-Protection header to all responses.
    # Unset if blank.
    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-XSS-Protection
    [ X-XSS-Protection: <string> ]
    # Set the Strict-Transport-Security header to HTTP responses.
    # Unset if blank.
    # Please make sure that you use this with care as this header might force
    # browsers to load Prometheus and the other applications hosted on the same
    # domain and subdomains over HTTPS.
    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Strict-Transport-Security
    [ Strict-Transport-Security: <string> ] ]

# Usernames and hashed passwords that have full access to the web
# server via basic authentication. If empty, no basic authentication is
# required. Passwords are hashed with bcrypt.
basic_auth_users:
  [ <string>: <secret> ... ]


# Rate limiting requests on the endpoint using a token bucket
#
# Requests are admitted through one shared token bucket refilled at
# `interval`, whatever the number of clients. Requests that arrive while the
# bucket is empty wait in a queue instead of failing immediately, and are then
# served in the order they arrived. Queues are kept per authenticated user
# when basic authentication is configured, and per client IP address
# otherwise, so one noisy client cannot consume another client's turn.
rate_limit:
  # Time between two tokens. Set to 0 (the default) to disable the limiter.
  [ interval: <duration> | default = 0 ]
  # Number of tokens the bucket can hold, i.e. how many requests may pass
  # back-to-back before requests start queueing.
  [ burst: <int> | default = 0 ]
  # How long one request may wait for a token before it is answered with
  # 429 Too Many Requests and a Retry-After header. Defaults to 5s.
  # A negative duration disables queueing: requests that find the bucket empty
  # are then rejected immediately, which is how earlier releases behaved.
  [ max_wait: <duration> | default = 5s ]
  # Maximum number of requests waiting per user or source address. Once a
  # queue is full, further requests from that identity are rejected with
  # 429 rather than growing the queue. 0 (the default) means no per-identity
  # bound; the shared bucket still bounds overall throughput.
  [ max_queued: <int> | default = 0 ]
  # Extra request paths served without rate limiting. Health and readiness
  # probes (/-/healthy, /-/ready, /healthz, /livez, /readyz and similar) are
  # always exempt. A trailing "*" exempts every path with that prefix.
  [ exempt_paths:
    [ - <string> ... ] ]
```

[A sample configuration file](web-config.yml) is provided.

## Rate limiter behaviour

When `rate_limit.interval` is set, every request consumes one token from a
single shared bucket, and the bucket refills one token per `interval`. The
bucket is shared by all clients and all listeners, so adding clients, opening
new connections, or sending credentials does not raise the accepted request
rate beyond `1/interval`.

Requests that arrive while the bucket is empty are queued instead of being
rejected straight away, which keeps a burst of parallel scrapes from turning
into a retry storm. Requests are queued:

* per user name, when the request carries basic auth credentials for a user
  listed in `basic_auth_users`;
* per client IP address otherwise, including requests without credentials and
  requests naming an unknown user.

Each queue is served in arrival order, and the limiter rotates between queues,
so a busy client cannot starve the others. A queued request that cannot be
served within `max_wait` is answered with `429 Too Many Requests` and a
`Retry-After` header; a queue that already holds `max_queued` requests rejects
new ones immediately with the same response. Setting `max_wait` to a negative
duration restores the earlier behaviour of rejecting as soon as the bucket is
empty.

Canceling a scrape (because the client went away) removes it from its queue at
once and does not consume a token. On server shutdown, all queued requests are
released with `503 Service Unavailable` and a `Retry-After` header.

Health and readiness probes are never queued: `/-/healthy`, `/-/ready`,
`/-/health`, `/-/live`, `/healthz`, `/livez`, `/readyz`, `/health` and `/ready`
are always served, plus any path listed in `exempt_paths`. A configured path
ending in `*` exempts every path with that prefix, for example `/debug/*`.

The limiter logs one line per decision with the queue identity (`user:<name>`
or `ip:<address>`), the decision, the time spent waiting and the number of
requests still waiting. Passwords and other credentials never appear in these
lines or in any metric.

When an exporter uses the toolkit's `bootstrap` package, the counters are also
exposed as metrics:

| Metric | Type | Description |
| --- | --- | --- |
| `exporter_web_rate_limit_requests_total` | counter | Requests by `decision` (`allowed`, `delayed`, `rejected`, `canceled`). |
| `exporter_web_rate_limit_wait_seconds_total` | counter | Total time requests spent queued. |
| `exporter_web_rate_limit_waiting_requests` | gauge | Requests currently queued. |
| `exporter_web_rate_limit_identity_requests_total` | counter | Per-identity requests by `decision`. |
| `exporter_web_rate_limit_identity_open` | gauge | 1 when an identity has requests queued. |

Other callers can read the same counters with `web.ServerRateLimitStats`.

## About bcrypt

There are several tools out there to generate bcrypt passwords, e.g.
[htpasswd](https://httpd.apache.org/docs/2.4/programs/htpasswd.html):

`htpasswd -nBC 10 "" | tr -d ':\n'`

That command will prompt you for a password and output the hashed password,
which will look something like:
`$2y$10$X0h1gDsPszWURQaxFh.zoubFi6DXncSjhoQNJgRrnGs7EsimhC7zG`

The cost (10 in the example) influences the time it takes for computing the
hash. A higher cost will end up slowing down the authentication process.
Depending on the machine, a cost of 10 will take about ~70ms, whereas a cost of
18 can take up to a few seconds. That hash will be computed on the first
authenticated HTTP request and then cached.

## Performance

Basic authentication is meant for simple use cases, with a few users.  If you
need to authenticate a lot of users, it is recommended to use TLS client
certificates, or to use a proper reverse proxy to handle the authentication.
