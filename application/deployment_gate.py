"""Temporary operator rollout barrier; native Stop/Run remains authoritative.

An operator creates the barrier before native submission, suspends the workspace,
verifies every pod has gone, and only then removes it. Subsequent GUI Run starts
normal application execution. No database, broker or model is contacted here.
"""
import argparse
import os
from pathlib import Path
import time

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--barrier',required=True)
    parser.add_argument('command',nargs=argparse.REMAINDER)
    args=parser.parse_args()
    root=Path(os.environ['ARTIFACT_ROOT']).resolve()/'.rollout-barriers'
    barrier=Path(args.barrier)
    if not barrier.is_absolute() or barrier.parent.resolve()!=root or barrier.is_symlink():
        raise ValueError('Rollout barrier must be directly inside the artifact rollout directory')
    command=args.command[1:] if args.command[:1]==['--'] else args.command
    if not command:raise ValueError('Application command required')
    print('rollout_barrier_waiting',flush=True)
    while barrier.exists():time.sleep(.2)
    print('rollout_barrier_released',flush=True)
    os.execvp(command[0],command)

if __name__=='__main__':main()
