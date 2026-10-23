import hashlib
import hmac
import re

from auraflow.core.exceptions import DAGValidationError

# Patterns that represent dangerous shell or filesystem escapes
DANGEROUS_PATTERNS = [
    re.compile(r"\.\./"),  # Path traversal
    re.compile(r";\s*rm\s+-rf"),  # Destructive shell deletion
    re.compile(r"__import__\s*\(\s*['\"]os['\"]\s*\)\.system"),  # Subprocess breakout
    re.compile(r"os\.system\s*\("),  # Direct shell invocation
    re.compile(r"subprocess\.Popen\s*\("),
]


class SecurityValidationError(DAGValidationError):
    """Raised when task parameters or script bodies violate safety constraints."""


class SecuritySanitizer:
    @staticmethod
    def validate_code_safety(code_str: str) -> None:
        """
        Inspects raw Python script contents for known exploit patterns
        and unsafe system invocations.
        """
        for pattern in DANGEROUS_PATTERNS:
            if pattern.search(code_str):
                raise SecurityValidationError(
                    f"Code failed security inspection: matched disallowed pattern '{pattern.pattern}'"
                )

    @staticmethod
    def sign_payload(secret_key: str, payload_bytes: bytes) -> str:
        """
        Generates an HMAC-SHA256 signature for task payload verification.
        """
        return hmac.new(
            secret_key.encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()

    @staticmethod
    def verify_signature(secret_key: str, payload_bytes: bytes, signature: str) -> bool:
        """
        Constant-time HMAC verification.
        """
        expected = hmac.new(
            secret_key.encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)
