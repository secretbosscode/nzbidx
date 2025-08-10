"""Install OpenSearch ILM policy and index template."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import ILM_DELETE_DAYS, ILM_WARM_DAYS
from nzbidx_common.os import OS_RELEASES_ALIAS


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
    settings["index.lifecycle.rollover_alias"] = OS_RELEASES_ALIAS
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

    try:
        alias_info = client.indices.get_alias(name=OS_RELEASES_ALIAS)
        if not any(
            d["aliases"].get(OS_RELEASES_ALIAS, {}).get("is_write_index")
            for d in alias_info.values()
        ):
            index_name = next(iter(alias_info))
            client.indices.put_alias(
                index=index_name, name=OS_RELEASES_ALIAS, is_write_index=True
            )
    except Exception:
        initial_index = f"{OS_RELEASES_ALIAS}-000001"
        if not client.indices.exists(index=initial_index):
            client.indices.create(
                index=initial_index,
                aliases={OS_RELEASES_ALIAS: {"is_write_index": True}},
            )
        else:
            client.indices.put_alias(
                index=initial_index, name=OS_RELEASES_ALIAS, is_write_index=True
            )
