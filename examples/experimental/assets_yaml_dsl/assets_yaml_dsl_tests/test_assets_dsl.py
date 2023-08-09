from typing import List

import yaml
from assets_yaml_dsl.assets_dsl import SomeSqlClient, from_asset_entries
from dagster import AssetsDefinition
from dagster._core.definitions.events import AssetKey


def assets_defs_from_yaml(yaml_string) -> List[AssetsDefinition]:
    return from_asset_entries(yaml.safe_load(yaml_string))


def test_basic() -> None:
    assets_defs = assets_defs_from_yaml("""
assets:
    - asset_key: asset_one
      sql: "SELECT * from asset_one"
""")
    assert assets_defs
    assert len(assets_defs) == 1
    assets_def = assets_defs[0]
    assert assets_def.key == AssetKey("asset_one")
    assert len(assets_def.keys) == 1


def test_single_dep() -> None:
    assets_defs = assets_defs_from_yaml("""
assets:
    - asset_key: key_ns/asset_one
      sql: "SELECT * from asset_one"
    - asset_key: key_ns/asset_two
      deps:
        - key_ns/asset_one
      sql: "SELECT * from asset_two"
""")
    assert assets_defs
    assert len(assets_defs) == 2
    asset_one = assets_defs[0]
    asset_two = assets_defs[1]

    assert asset_one.key == AssetKey.from_user_string("key_ns/asset_one")
    assert asset_two.key == AssetKey.from_user_string("key_ns/asset_two")

    assert asset_two.asset_deps[asset_two.key] == {asset_one.key}


def test_description() -> None:
    assets_defs = assets_defs_from_yaml("""
assets:
    - asset_key: asset_one
      description: "asset one description"
      sql: "SELECT * from asset_one"
""")
    assert assets_defs
    assert len(assets_defs) == 1
    assets_def = assets_defs[0]
    assert assets_def.key == AssetKey("asset_one")
    assert assets_def.descriptions_by_key[assets_def.key] == "asset one description"


def test_execution() -> None:
    assets_defs = assets_defs_from_yaml("""
assets:
    - asset_key: asset_one
      sql: "SELECT * from asset_one"
""")
    assert assets_defs
    assert len(assets_defs) == 1
    assets_def = assets_defs[0]
    sql_client = SomeSqlClient()
    assets_def(sql_client=sql_client)
    assert sql_client.queries == ["SELECT * from asset_one"]


def test_basic_group() -> None:
    assets_defs = assets_defs_from_yaml("""
group_name: my_group
assets:
    - asset_key: asset_one
      sql: "SELECT * from asset_one"
""")
    assert assets_defs
    assert len(assets_defs) == 1
    assets_def = assets_defs[0]
    assert assets_def.group_names_by_key[assets_def.key] == "my_group"
