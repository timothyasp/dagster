from typing import NamedTuple, Optional

from .events import (
    AssetKey,
    CoercibleToAssetKey,
)
from .metadata import MetadataUserInput


class MaterializeResult(
    NamedTuple(
        "_MaterializeResult",
        [
            ("asset_key", Optional[AssetKey]),
            ("metadata", Optional[MetadataUserInput]),
        ],
    )
):
    def __new__(
        cls,
        *,  # enforce kwargs
        asset_key: Optional[CoercibleToAssetKey] = None,
        metadata: Optional[MetadataUserInput] = None,
    ):
        asset_key = AssetKey.from_coercible(asset_key) if asset_key else None

        return super().__new__(
            cls,
            asset_key=asset_key,
            metadata=metadata,  # check?
        )
