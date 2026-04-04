import secrets, hashlib, base64

def generate_pkce():
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).rstrip(b"=").decode("ascii")
    return code_verifier, code_challenge

state = secrets.token_urlsafe(32)
code_verifier, code_challenge = generate_pkce()

print("STATE:", state)
print("CODE_VERIFIER:", code_verifier)
print("CODE_CHALLENGE:", code_challenge)