"""A rollout barrier must prevent any application startup until released."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

class DeploymentGateTests(unittest.TestCase):
    def test_child_never_starts_while_barrier_exists_then_starts_on_release(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);barrier=root/'.rollout-barriers'/'test';barrier.parent.mkdir();barrier.touch();marker=root/'started'
            cmd=[sys.executable,'-m','application.deployment_gate','--barrier',str(barrier),'--',sys.executable,'-c','from pathlib import Path; Path('+repr(str(marker))+').touch()']
            process=subprocess.Popen(cmd,env={**os.environ,'ARTIFACT_ROOT':str(root)},stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            try:
                time.sleep(.3);self.assertIsNone(process.poll());self.assertFalse(marker.exists())
                barrier.unlink();self.assertEqual(process.wait(timeout=3),0);self.assertTrue(marker.exists())
            finally:
                if process.poll() is None:process.terminate();process.wait(timeout=3)
    def test_stopping_held_worker_never_starts_child(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);barrier=root/'.rollout-barriers'/'test';barrier.parent.mkdir();barrier.touch();marker=root/'started'
            process=subprocess.Popen([sys.executable,'-m','application.deployment_gate','--barrier',str(barrier),'--',sys.executable,'-c','from pathlib import Path; Path('+repr(str(marker))+').touch()'],env={**os.environ,'ARTIFACT_ROOT':str(root)},stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            try:
                time.sleep(.3);self.assertIsNone(process.poll());process.terminate();process.wait(timeout=3);self.assertFalse(marker.exists())
            finally:
                if process.poll() is None:process.kill();process.wait()
