"""hydrowf_core.auth: which bearer tokens give claims, where the keys come from, and the groups a token names."""
import io
import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from jwt.exceptions import PyJWKClientConnectionError, PyJWKClientError

from hydrowf_core import auth

ISSUER = "https://keycloak.test/realms/hydrowf"
AUDIENCE = "hydrowf-api"
ME = "8d3a6e2c-1f0b-4c4e-9a51-0e6d2b7c9f10"                                 # a Keycloak sub
KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)       # the issuer's signing key
OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
SIGNING_KEY = jwt.PyJWK({**RSAAlgorithm.to_jwk(KEY.public_key(), as_dict=True), "alg": "RS256", "kid": "test"})


def token(key=KEY, **claims):
    """A token minted now (inside the test, so a slow suite can't expire it)."""
    now = int(time.time())
    body = {"iss": ISSUER, "aud": AUDIENCE, "sub": ME, "iat": now, "exp": now + 300, **claims}
    return jwt.encode({k: v for k, v in body.items() if v is not None}, key, algorithm="RS256",
                      headers={"kid": "test"})


class StubKeys:
    """The issuer's JWKS as PyJWKClient serves it: the signing key, or the error a broken issuer gives."""

    def __init__(self, error=None):
        self.error = error

    def get_signing_key_from_jwt(self, token):
        if self.error:
            raise self.error
        return SIGNING_KEY


def bearer(error=None):
    return auth.BearerAuth(ISSUER, AUDIENCE, keys=StubKeys(error))


# ---- claims: valid tokens and everything else


@pytest.mark.parametrize("claims", [
    {},
    {"aud": ["account", AUDIENCE]},                       # Keycloak's aud: a list with ours in it
    {"iat": int(time.time()) + 20},                       # the issuer's clock 20 s ahead: within LEEWAY
], ids=["plain", "keycloak-aud-list", "clock-skew"])
def test_valid_tokens_give_their_claims(claims):
    with_slash = auth.BearerAuth(ISSUER + "/", AUDIENCE, keys=StubKeys())   # a trailing slash is dropped
    assert with_slash.claims(token(**claims))["sub"] == ME


@pytest.mark.parametrize("make", [
    lambda: token(key=OTHER_KEY),
    lambda: token(exp=int(time.time()) - 120),
    lambda: token(aud="someone-else"),
    lambda: token(iss="https://evil.test"),
    lambda: token(sub=None),
    lambda: token(exp=None),
    lambda: "abc",
], ids=["wrong-key", "expired", "other-audience", "other-issuer", "no-sub", "no-exp", "not-a-jwt"])
def test_tokens_not_for_us_have_no_claims(make):
    assert bearer().claims(make()) is None


def test_a_token_whose_key_the_issuer_lacks_has_no_claims():
    assert bearer(PyJWKClientError("no matching kid")).claims(token()) is None


def test_an_unreachable_issuer_is_an_error_not_a_bad_token():
    with pytest.raises(PyJWKClientConnectionError):
        bearer(PyJWKClientConnectionError("down")).claims(token())


def test_a_string_that_is_not_a_jwt_never_asks_the_issuer(monkeypatch):
    def no_discovery(issuer, timeout=None):
        raise AssertionError("discovery for a token that isn't a JWT")
    monkeypatch.setattr(auth, "discover_jwks_uri", no_discovery)
    assert auth.BearerAuth(ISSUER, AUDIENCE).claims("abc") is None


# ---- where the keys come from


def discovery(document):
    return lambda url, timeout: io.BytesIO(json.dumps(document).encode())


def test_discovery_gives_the_jwks_uri(monkeypatch):
    monkeypatch.setattr(auth, "urlopen", discovery({"issuer": ISSUER, "jwks_uri": f"{ISSUER}/certs"}))
    assert auth.discover_jwks_uri(ISSUER) == f"{ISSUER}/certs"


def test_discovery_must_name_our_issuer(monkeypatch):
    monkeypatch.setattr(auth, "urlopen", discovery({"issuer": "https://evil.test", "jwks_uri": "x"}))
    with pytest.raises(auth.AuthConfigError):
        auth.discover_jwks_uri(ISSUER)


def test_an_issuer_without_an_audience_fails_at_startup():
    with pytest.raises(auth.AuthConfigError):
        auth.bearer_from_env({"HYDROWF_OIDC_ISSUER": ISSUER})
    assert auth.bearer_from_env({}) is None
    assert auth.bearer_from_env({"HYDROWF_OIDC_ISSUER": ISSUER, "HYDROWF_OIDC_AUDIENCE": AUDIENCE}).issuer == ISSUER


# ---- the Authorization header, and the groups a token names


@pytest.mark.parametrize("header, token_", [
    (None, None),
    ("Basic dXNlcjpwYXNz", None),          # another scheme: no bearer credential
    ("Bearer", ""),                        # a bearer header without a token: refused, never anonymous
    ("bearer  abc ", "abc"),
])
def test_bearer_token_only_extracts(header, token_):
    assert auth.bearer_token(header) == token_


@pytest.mark.parametrize("claims, groups", [
    ({"groups": ["/tool-authors", "/data/editors"]}, {"tool-authors", "data/editors"}),   # "Full group path" on
    ({"groups": ["tool-authors"]}, {"tool-authors"}),                                     # off
    ({}, set()),
    ({"groups": "tool-authors"}, set()),                                                  # not a list
    ({"groups": ["/", "", 3]}, set()),
], ids=["full-path", "names", "no-claim", "not-a-list", "junk"])
def test_groups_from(claims, groups):
    assert auth.groups_from(claims) == groups
