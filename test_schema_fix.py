#!/usr/bin/env python3
"""Test script to verify the schema fixes work correctly."""

import asyncio
import sys
from sqlmodel import select
from app.db_postgres import get_session
from app.models.postgres import Domain, PortService, SSLData


async def test_schema_fixes():
    """Test that both PortService and SSLData models work with the database."""
    print("Testing schema fixes...")
    
    try:
        async for session in get_session():
            # Test PortService with 'service' field
            print("1. Testing PortService model with 'service' field...")
            port_stmt = select(PortService).limit(1)
            port_result = await session.exec(port_stmt)
            port = port_result.first()
            if port:
                print(f"   ✓ Found port service: port={port.port}, service={port.service}")
            else:
                print("   ✓ No port services found, but query executed successfully")
            
            # Test SSLData without 'signature_algorithm' field
            print("2. Testing SSLData model without 'signature_algorithm' field...")
            ssl_stmt = select(SSLData).limit(1)
            ssl_result = await session.exec(ssl_stmt)
            ssl = ssl_result.first()
            if ssl:
                print(f"   ✓ Found SSL data: serial={ssl.serial_number}, issuer={ssl.issuer_common_name}")
            else:
                print("   ✓ No SSL data found, but query executed successfully")
            
            # Test Domain with relationships
            print("3. Testing Domain model with relationships...")
            domain_stmt = select(Domain).limit(1)
            domain_result = await session.exec(domain_stmt)
            domain = domain_result.first()
            if domain:
                print(f"   ✓ Found domain: {domain.name}")
            else:
                print("   ✓ No domains found, but query executed successfully")
            
            print("\n✅ All schema fixes verified successfully!")
            return True
            
    except Exception as e:
        print(f"\n❌ Schema test failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_schema_fixes())
    sys.exit(0 if success else 1)