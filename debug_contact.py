#!/usr/bin/env python3

"""Debug script to isolate the contact route issue."""

from fastapi.testclient import TestClient
from app.main import app
from app.deps import get_postgres_session

# Override the database session
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

app.dependency_overrides[get_postgres_session] = dummy_session

# Test the contact route with minimal payload
client = TestClient(app)

response = client.post(
    "/contact",
    json={
        "name": "Test",
        "email": "test@example.com", 
        "subject": "Debug",
        "message": "Testing"
    }
)

print(f"Status Code: {response.status_code}")
print(f"Response: {response.text}")
print(f"Headers: {dict(response.headers)}")

# Check if the route is properly registered
for route in app.routes:
    if hasattr(route, 'path') and '/contact' in route.path:
        print(f"Found route: {route.path} - {route.methods}")
        if hasattr(route, 'dependant'):
            print(f"Dependencies: {route.dependant}")