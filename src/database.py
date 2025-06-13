# database.py
"""Database operations for Supabase"""
import asyncio
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime
import backoff
from supabase import create_client, Client
import httpx

from src.config import (
    SUPABASE_URL, SUPABASE_KEY, FETCH_BATCH_SIZE,
    DB_MAX_RETRIES, DB_RETRY_DELAY, DB_TIMEOUT,
    MAX_CONNECTIONS_PER_WORKER
)
from src.models import Study

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom database exception"""
    pass


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, worker_id: int = 0):
        self.worker_id = worker_id
        self.client = self._create_client()
        self._connection_semaphore = asyncio.Semaphore(MAX_CONNECTIONS_PER_WORKER)
    
    def _create_client(self) -> Client:
        """Create a Supabase client"""
        # Create a basic client without custom httpx settings for now
        # The supabase-py library handles its own connection pooling
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    
    @backoff.on_exception(
        backoff.expo,
        Exception,  # Catch general exceptions for now
        max_tries=DB_MAX_RETRIES,
        max_time=60
    )
    async def fetch_unanalyzed_studies(
        self, 
        offset: int = 0, 
        limit: int = FETCH_BATCH_SIZE,
        supplement_id: Optional[int] = None
    ) -> List[Study]:
        """Fetch unanalyzed studies from database"""
        async with self._connection_semaphore:
            try:
                query = self.client.table("supplement_studies") \
                    .select("id, supplement_id, pmid, title, abstract") \
                    .is_("last_analyzed_at", "null") \
                    .order("supplement_id", desc=False) \
                    .order("id", desc=False) \
                    .range(offset, offset + limit - 1)
                
                if supplement_id:
                    query = query.eq("supplement_id", supplement_id)
                
                # Execute in thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(None, query.execute)
                
                # Get supplement names for the studies
                studies = []
                supplement_cache = {}
                
                for row in response.data:
                    # Get supplement name (with caching)
                    supp_id = row['supplement_id']
                    if supp_id not in supplement_cache:
                        supp_response = await loop.run_in_executor(
                            None,
                            lambda: self.client.table("supplements")
                                .select("name")
                                .eq("id", supp_id)
                                .single()
                                .execute()
                        )
                        supplement_cache[supp_id] = supp_response.data['name']
                    
                    study = Study(
                        id=row['id'],
                        supplement_id=supp_id,
                        supplement_name=supplement_cache[supp_id],
                        pmid=row.get('pmid'),
                        title=row.get('title'),
                        abstract=row.get('abstract')
                    )
                    studies.append(study)
                
                logger.info(f"Worker {self.worker_id}: Fetched {len(studies)} studies (offset: {offset})")
                return studies
                
            except Exception as e:
                logger.error(f"Worker {self.worker_id}: Failed to fetch studies: {e}")
                raise DatabaseError(f"Failed to fetch studies: {e}")
    
    async def get_total_unanalyzed_count(self) -> int:
        """Get total count of unanalyzed studies"""
        async with self._connection_semaphore:
            try:
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.client.table("supplement_studies")
                        .select("id", count="exact")
                        .is_("last_analyzed_at", "null")
                        .execute()
                )
                
                count = response.count if response.count is not None else 0
                logger.info(f"Total unanalyzed studies: {count}")
                return count
                
            except Exception as e:
                logger.error(f"Failed to get unanalyzed count: {e}")
                raise DatabaseError(f"Failed to get count: {e}")
    
    async def batch_update_studies(self, updates: List[Dict[str, Any]]) -> bool:
        """Batch update studies with analysis results"""
        async with self._connection_semaphore:
            try:
                # Supabase doesn't have native batch update, so we'll use upsert
                # Transform updates to include the id field
                upsert_data = []
                for update in updates:
                    data = update.copy()
                    # Ensure we have the study_id as 'id' for upsert
                    if 'study_id' in data:
                        data['id'] = data.pop('study_id')
                    upsert_data.append(data)
                
                # Execute in thread pool
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.client.table("supplement_studies")
                        .upsert(upsert_data)
                        .execute()
                )
                
                logger.info(f"Successfully updated {len(updates)} studies")
                return True
                
            except Exception as e:
                logger.error(f"Failed to batch update studies: {e}")
                return False
    
    def close(self):
        """Close database connection"""
        # Supabase client doesn't need explicit closing in the current version
        logger.debug(f"Database connection closed for worker {self.worker_id}")


class DatabasePool:
    """Manages a pool of database connections"""
    
    def __init__(self, pool_size: int = 5):
        self.pool_size = pool_size
        self.connections: List[DatabaseManager] = []
        self._create_pool()
    
    def _create_pool(self):
        """Create connection pool"""
        for i in range(self.pool_size):
            conn = DatabaseManager(worker_id=i)
            self.connections.append(conn)
        logger.info(f"Created database pool with {self.pool_size} connections")
    
    async def get_connection(self) -> DatabaseManager:
        """Get an available connection from pool"""
        # Simple round-robin for now
        # In production, you might want a more sophisticated algorithm
        import random
        return random.choice(self.connections)
    
    def close_all(self):
        """Close all connections in pool"""
        for conn in self.connections:
            conn.close()
        logger.info("Closed all database connections")