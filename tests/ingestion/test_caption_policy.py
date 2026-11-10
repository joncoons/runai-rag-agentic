import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from nv_ingest_durable.caption_policy import CaptionPolicyProcessor


class CaptionPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def run_policy(self,metadata):
        pool=SimpleNamespace(fetchrow=AsyncMock(return_value={'metadata':metadata}))
        enabled=SimpleNamespace(process=AsyncMock(return_value='enabled'))
        disabled=SimpleNamespace(process=AsyncMock(return_value='disabled'))
        policy=CaptionPolicyProcessor(pool,enabled,disabled)
        result=await policy.process(SimpleNamespace(job_id='00000000-0000-0000-0000-000000000001'))
        return result,enabled,disabled

    async def test_default_is_no_caption_work(self):
        result,enabled,disabled=await self.run_policy({})
        self.assertEqual(result,'disabled');enabled.process.assert_not_called()

    async def test_explicit_opt_out_does_not_activate_caption_processor(self):
        result,enabled,disabled=await self.run_policy({'ingestion_options':{'captions_enabled':False}})
        self.assertEqual(result,'disabled');enabled.process.assert_not_called()

    async def test_explicit_user_opt_in_activates_caption_processor(self):
        result,enabled,disabled=await self.run_policy({'ingestion_options':{'captions_enabled':True}})
        self.assertEqual(result,'enabled');disabled.process.assert_not_called()

    async def test_non_boolean_option_is_rejected(self):
        with self.assertRaises(ValueError):await self.run_policy({'ingestion_options':{'captions_enabled':'true'}})
