from typing import AbstractSet, Any, Iterable, Mapping, NamedTuple, Optional, Union

import dagster._check as check

from .auto_materialize_policy import AutoMaterializePolicy
from .events import (
    AssetKey,
    CoercibleToAssetKey,
)
from .freshness_policy import FreshnessPolicy
from .metadata import MetadataUserInput


class AssetNode(
    NamedTuple(
        "_AssetNode",
        [
            ("asset_key", AssetKey),
            ("deps", AbstractSet[AssetKey]),
            ("description", Optional[str]),
            ("metadata", Optional[Mapping[str, Any]]),
            ("group_name", Optional[str]),
            ("code_version", Optional[str]),
            ("freshness_policy", Optional[FreshnessPolicy]),
            ("auto_materialize_policy", Optional[AutoMaterializePolicy]),
        ],
    )
):
    def __new__(
        cls,
        asset_key: CoercibleToAssetKey,
        deps: Optional[
            Iterable[
                Union[
                    CoercibleToAssetKey,
                    "AssetNode",
                    # AssetsDefinition, if only one-key trick
                ]
            ]
        ] = None,
        description: Optional[str] = None,
        metadata: Optional[MetadataUserInput] = None,
        group_name: Optional[str] = None,
        code_version: Optional[str] = None,
        freshness_policy: Optional[FreshnessPolicy] = None,
        auto_materialize_policy: Optional[AutoMaterializePolicy] = None,
    ):
        dep_set = set()
        if deps:
            for dep in deps:
                if isinstance(dep, AssetNode):
                    dep_set.add(dep.asset_key)
                else:
                    dep_set.add(AssetKey.from_coercible(asset_key))

        return super().__new__(
            cls,
            asset_key=AssetKey.from_coercible(asset_key),
            deps=dep_set,
            description=check.opt_str_param(description, "description"),
            metadata=check.opt_mapping_param(metadata, "metadata", key_type=str),
            group_name=check.opt_str_param(group_name, "group_name"),
            code_version=check.opt_str_param(code_version, "code_version"),
            freshness_policy=check.opt_inst_param(
                freshness_policy, "freshness_policy", FreshnessPolicy
            ),
            auto_materialize_policy=check.opt_inst_param(
                auto_materialize_policy, "auto_materialize_policy", AutoMaterializePolicy
            ),
        )
