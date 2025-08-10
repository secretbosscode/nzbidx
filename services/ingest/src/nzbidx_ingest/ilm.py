"""Install OpenSearch ILM policy and index template."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import ILM_DELETE_DAYS, ILM_WARM_DAYS


def build_policy() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[4] / "opensearch" / "ilm-policy.json"
    policy = json.loads(path.read_text("utf-8"))
    policy["policy"]["phases"]["warm"]["min_age"] = f"{ILM_WARM_DAYS}d"
    policy["policy"]["phases"]["delete"]["min_age"] = f"{ILM_DELETE_DAYS}d"
    return policy


def build_template() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[4] / "opensearch" / "index-template.json"
    template = json.loads(path.read_text("utf-8"))
    settings = template.setdefault("template", {}).setdefault("settings", {})
    settings.setdefault("refresh_interval", "5s")
    settings["index.lifecycle.name"] = "nzbidx-releases-policy"
    settings["index.lifecycle.rollover_alias"] = "nzbidx-releases"
    return template


def install(client) -> None:
    try:
        client.ilm.get_lifecycle(name="nzbidx-releases-policy")
    except Exception:
        client.ilm.put_lifecycle(name="nzbidx-releases-policy", body=build_policy())

    if not client.indices.exists_index_template(name="nzbidx-releases-template"):
        client.indices.put_index_template(
            name="nzbidx-releases-template", body=build_template()
        )

    if not client.indices.exists(index="nzbidx-releases-000001"):
        client.indices.create(
            index="nzbidx-releases-000001",
            aliases={"nzbidx-releases": {"is_write_index": True}},
        )
