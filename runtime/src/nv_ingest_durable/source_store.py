"""Content-addressed local source artifact import."""

from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from .contracts import CommittedArtifact


@dataclass(slots=True)
class FileSourceStore:
    root: Path
    io_concurrency: int = 4
    _io_slots: asyncio.Semaphore = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.io_concurrency < 1:
            raise ValueError("io_concurrency must be positive")
        self.root = self.root.resolve()
        (self.root / ".pending").mkdir(parents=True, exist_ok=True)
        (self.root / "sources").mkdir(parents=True, exist_ok=True)
        self._io_slots = asyncio.Semaphore(self.io_concurrency)

    async def import_path(
        self, path: Path, *, metadata: Mapping[str, Any]
    ) -> CommittedArtifact:
        async with self._io_slots:
            return await asyncio.to_thread(self._import_path, path, metadata)

    def _import_path(
        self, path: Path, metadata: Mapping[str, Any]
    ) -> CommittedArtifact:
        source = path.resolve()
        if not source.is_file():
            raise FileNotFoundError(source)
        temporary = self.root / ".pending" / str(uuid4())
        digest = hashlib.sha256()
        size = 0
        try:
            with source.open("rb") as reader, temporary.open("xb") as writer:
                while block := reader.read(1024 * 1024):
                    writer.write(block)
                    digest.update(block)
                    size += len(block)
                writer.flush()
                os.fsync(writer.fileno())
            digest_hex = digest.hexdigest()
            final = self.root / "sources" / digest_hex[:2] / digest_hex
            final.parent.mkdir(parents=True, exist_ok=True)
            if final.exists():
                temporary.unlink()
            else:
                os.replace(temporary, final)
                directory_fd = os.open(final.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            if temporary.exists():
                temporary.unlink()

        artifact_metadata = {
            "original_name": source.name,
            "original_path": str(source),
            **dict(metadata),
        }
        return CommittedArtifact(
            uri=final.as_uri(),
            digest=f"sha256:{digest_hex}",
            size_bytes=size,
            metadata=artifact_metadata,
        )
