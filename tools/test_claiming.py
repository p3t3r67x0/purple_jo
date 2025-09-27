#!/usr/bin/env python3
"""
Test script to verify the domain claiming functionality works correctly.
"""

import asyncio
import sys
import os

# Add the project root to the Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.crawl_urls_postgres import PostgresAsync


async def test_claiming(postgres_dsn: str):
    """Test the domain claiming functionality."""
    print("Testing domain claiming functionality...")
    
    # Create two postgres instances to simulate multiple workers
    postgres1 = PostgresAsync(postgres_dsn)
    postgres2 = PostgresAsync(postgres_dsn)
    
    test_domain = "test-claim-domain.com"
    
    try:
        # Clean up any existing test data
        async with await postgres1.get_session() as session1:
            from sqlalchemy import text
            await session1.execute(text("DELETE FROM crawl_status WHERE domain_name = :domain"), 
                                 {"domain": test_domain})
            await session1.commit()
        
        # Test claiming
        async with await postgres1.get_session() as session1:
            async with await postgres2.get_session() as session2:
                # First worker claims domain
                stmt = text("""
                    INSERT INTO crawl_status (domain_name, created_at, updated_at)
                    VALUES (:domain, NOW(), NOW())
                    ON CONFLICT (domain_name) DO NOTHING
                    RETURNING id
                """)
                
                result1 = await session1.execute(stmt, {"domain": test_domain})
                claim1 = result1.fetchone() is not None
                await session1.commit()
                
                # Second worker tries to claim same domain
                result2 = await session2.execute(stmt, {"domain": test_domain})
                claim2 = result2.fetchone() is not None
                await session2.commit()
                
                print(f"Worker 1 claim result: {claim1}")
                print(f"Worker 2 claim result: {claim2}")
                
                if claim1 and not claim2:
                    print("✅ Domain claiming works correctly!")
                else:
                    print("❌ Domain claiming failed!")
        
        # Clean up
        async with await postgres1.get_session() as session1:
            await session1.execute(text("DELETE FROM crawl_status WHERE domain_name = :domain"), 
                                 {"domain": test_domain})
            await session1.commit()
            
    finally:
        await postgres1.close()
        await postgres2.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_claiming.py <postgres_dsn>")
        sys.exit(1)
    
    postgres_dsn = sys.argv[1]
    asyncio.run(test_claiming(postgres_dsn))