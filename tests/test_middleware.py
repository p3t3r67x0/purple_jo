"""Tests for middleware utilities."""

from app.middleware import _anonymize_remote_address


def test_anonymize_remote_address_ipv4():
    """Masks IPv4 addresses to their /24 network."""
    assert _anonymize_remote_address("203.0.113.42") == "203.0.113.0"


def test_anonymize_remote_address_ipv6():
    """Masks IPv6 addresses to their /64 network."""
    assert (
        _anonymize_remote_address("2001:db8:85a3::8a2e:370:7334")
        == "2001:db8:85a3::"
    )


def test_anonymize_remote_address_with_ports_and_multiple_entries():
    """Strips port information and masks each comma-separated address."""
    masked = _anonymize_remote_address("203.0.113.42:443, 198.51.100.10")
    assert masked == "203.0.113.0, 198.51.100.0"


def test_anonymize_remote_address_ipv6_brackets():
    """Handles IPv6 addresses enclosed in brackets with ports."""
    assert _anonymize_remote_address("[2001:db8::1]:8080") == "2001:db8::"


def test_anonymize_remote_address_invalid():
    """Returns None when no valid IP addresses are provided."""
    assert _anonymize_remote_address("invalid-value") is None
