#!/usr/bin/env python3
"""Debug script to understand why one contact test works and another doesn't."""

import os
import sys
import json
sys.path.insert(0, '/home/awendelk/git/purple_jo')

from fastapi.testclient import TestClient
from app.main import app
from app.deps import get_postgres_session, get_client_ip
from app.settings import reset_settings_cache

def setup_working_client():
    """Setup client the same way as test_routes_api.py."""
    app.dependency_overrides.clear()

    class MockSession:
        async def rollback(self):
            pass
        async def commit(self):
            pass
        async def flush(self):
            pass
        def add(self, obj):
            pass

    async def dummy_session(request):
        yield MockSession()
    
    def dummy_client_ip():
        return "127.0.0.1"

    app.dependency_overrides[get_postgres_session] = dummy_session
    app.dependency_overrides[get_client_ip] = dummy_client_ip

    return TestClient(app)

def setup_failing_client():
    """Setup client the same way as test_contact_captcha.py."""
    app.dependency_overrides.clear()

    class MockSession:
        async def rollback(self):
            pass
        async def commit(self):
            pass
        async def flush(self):
            pass
        def add(self, obj):
            pass

    async def dummy_session(request):
        yield MockSession()

    def dummy_client_ip(request):
        return "127.0.0.1"

    app.dependency_overrides[get_postgres_session] = dummy_session
    app.dependency_overrides[get_client_ip] = dummy_client_ip

    return TestClient(app)

# Test working setup
print("=== Testing working setup ===")
client1 = setup_working_client()
response1 = client1.post("/contact", json={
    "name": "Alice",
    "email": "alice@example.com",
    "subject": "Hello",
    "message": "Testing",
})
print(f"Working setup - Status: {response1.status_code}")
print(f"Working setup - Response: {response1.text}")

# Test failing setup
print("\n=== Testing failing setup ===")
os.environ["CONTACT_TOKEN"] = "t123"
reset_settings_cache()
client2 = setup_failing_client()
response2 = client2.post("/contact", json={
    "name": "Alice",
    "email": "alice@example.com", 
    "subject": "Hi",
    "message": "Hello world message",
    "token": "t123"
})
print(f"Failing setup - Status: {response2.status_code}")
print(f"Failing setup - Response: {response2.text}")

app.dependency_overrides.clear()