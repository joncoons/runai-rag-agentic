"""Packaged SQL migration discovery."""

from __future__ import annotations

from importlib.resources import files


def read_migrations() -> list[tuple[str, str]]:
    directory = files("nv_ingest_durable").joinpath("migrations")
    migrations = []
    for resource in sorted(directory.iterdir(), key=lambda item: item.name):
        if resource.name.endswith(".sql"):
            migrations.append((resource.name, resource.read_text(encoding="utf-8")))
    return migrations
