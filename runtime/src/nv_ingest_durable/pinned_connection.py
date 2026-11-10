"""Share one caller-owned transaction with existing pool-based repositories."""
from contextlib import asynccontextmanager


class PinnedPool:
    def __init__(self,connection):
        self.connection=connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection
