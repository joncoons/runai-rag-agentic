"""Recover abandoned/failed work even when a stream has no new arrivals.

Keep the original bounded worker, leases, attempts, reclaim cursor and delivery
semantics. Its fresh-message counter alone cannot schedule idle recovery.
"""
import asyncio
from .service_original import StageWorkerService as _OriginalStageWorkerService

class StageWorkerService(_OriginalStageWorkerService):
    async def _claim_one(self,*,reclaim,block_ms=None):
        now=asyncio.get_running_loop().time()
        due=now>=getattr(self,'_recovery_due',0)
        if due:
            self._recovery_due=now+max(.1,min(5.0,self.reclaim_idle_ms/2000))
        return await super()._claim_one(reclaim=reclaim or due,block_ms=block_ms)
