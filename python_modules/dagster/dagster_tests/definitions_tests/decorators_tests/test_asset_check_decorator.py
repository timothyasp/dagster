import re
from typing import NamedTuple

import pytest
from dagster import (
    AssetCheckResult,
    AssetKey,
    Definitions,
    ExecuteInProcessResult,
    IOManager,
    MetadataValue,
    ResourceParam,
    asset,
    asset_check,
    define_asset_job,
)
from dagster._core.errors import DagsterInvalidDefinitionError, DagsterInvariantViolationError


def execute_assets_and_checks(
    assets=None, asset_checks=None, raise_on_error: bool = True, resources=None
) -> ExecuteInProcessResult:
    defs = Definitions(assets=assets, asset_checks=asset_checks, resources=resources)
    job_def = defs.get_implicit_global_asset_job_def()
    return job_def.execute_in_process(raise_on_error=raise_on_error)


def test_asset_check_decorator():
    @asset_check(asset="asset1", description="desc")
    def check1():
        ...

    assert check1.name == "check1"
    assert check1.description == "desc"
    assert check1.asset_key == AssetKey("asset1")


def test_asset_check_decorator_name():
    @asset_check(asset="asset1", description="desc", name="check1")
    def _check():
        ...

    assert _check.name == "check1"


def test_execute_asset_and_check():
    @asset
    def asset1():
        ...

    @asset_check(asset=asset1, description="desc")
    def check1(context):
        assert context.asset_key_for_input("asset1") == asset1.key
        asset_check_spec = context.asset_check_spec
        return AssetCheckResult(
            asset_key=asset_check_spec.asset_key,
            check_name=asset_check_spec.name,
            success=True,
            metadata={"foo": "bar"},
        )

    result = execute_assets_and_checks(assets=[asset1], asset_checks=[check1])
    assert result.success

    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 1
    check_eval = check_evals[0]
    assert check_eval.asset_key == asset1.key
    assert check_eval.check_name == "check1"
    assert check_eval.metadata == {"foo": MetadataValue.text("bar")}


def test_execute_check_without_asset():
    @asset_check(asset="asset1", description="desc")
    def check1():
        return AssetCheckResult(success=True, metadata={"foo": "bar"})

    result = execute_assets_and_checks(asset_checks=[check1])
    assert result.success

    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 1
    check_eval = check_evals[0]
    assert check_eval.asset_key == AssetKey("asset1")
    assert check_eval.check_name == "check1"
    assert check_eval.metadata == {"foo": MetadataValue.text("bar")}


def test_execute_check_and_unrelated_asset():
    @asset
    def asset2():
        ...

    @asset_check(asset="asset1", description="desc")
    def check1():
        return AssetCheckResult(success=True)

    result = execute_assets_and_checks(assets=[asset2], asset_checks=[check1])
    assert result.success

    materialization_events = result.get_asset_materialization_events()
    assert len(materialization_events) == 1

    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 1
    check_eval = check_evals[0]
    assert check_eval.asset_key == AssetKey("asset1")
    assert check_eval.check_name == "check1"


def test_check_doesnt_execute_if_asset_fails():
    check_executed = [False]

    @asset
    def asset1():
        raise ValueError()

    @asset_check(asset=asset1)
    def asset1_check(context):
        check_executed[0] = True

    result = execute_assets_and_checks(
        assets=[asset1], asset_checks=[asset1_check], raise_on_error=False
    )
    assert not result.success

    assert not check_executed[0]


def test_check_decorator_unexpected_asset_key():
    @asset_check(asset="asset1", description="desc")
    def asset1_check():
        return AssetCheckResult(asset_key=AssetKey("asset2"), success=True)

    with pytest.raises(
        DagsterInvariantViolationError,
        match=re.escape(
            "Received unexpected AssetCheckResult. It targets asset 'asset2' which is not targeted"
            " by any of the checks currently being evaluated. Targeted assets: ['asset1']."
        ),
    ):
        execute_assets_and_checks(asset_checks=[asset1_check])


def test_asset_check_separate_op_downstream_still_executes():
    @asset
    def asset1():
        ...

    @asset_check(asset=asset1)
    def asset1_check(context):
        return AssetCheckResult(success=False)

    @asset(deps=[asset1])
    def asset2():
        ...


def test_definitions_conflicting_checks():
    def make_check():
        @asset_check(asset="asset1")
        def check1(context):
            ...

        return check1

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match='Detected conflicting node definitions with the same name "asset1_check1"',
    ):
        Definitions(asset_checks=[make_check(), make_check()])


def test_definitions_same_name_different_asset():
    def make_check_for_asset(asset_key: str):
        @asset_check(asset=asset_key)
        def check1(context):
            ...

        return check1

    Definitions(asset_checks=[make_check_for_asset("asset1"), make_check_for_asset("asset2")])


def test_definitions_same_asset_different_name():
    def make_check(check_name: str):
        @asset_check(asset="asset1", name=check_name)
        def _check(context):
            ...

        return _check

    Definitions(asset_checks=[make_check("check1"), make_check("check2")])


def test_resource_params():
    class MyResource(NamedTuple):
        value: int

    @asset_check(asset=AssetKey("asset1"))
    def check1(my_resource: ResourceParam[MyResource]):
        assert my_resource.value == 5
        return AssetCheckResult(success=True)

    execute_assets_and_checks(asset_checks=[check1], resources={"my_resource": MyResource(5)})


def test_job_only_execute_checks_downstream_of_selected_assets():
    @asset
    def asset1():
        ...

    @asset
    def asset2():
        ...

    @asset_check(asset=asset1)
    def check1():
        return AssetCheckResult(success=False)

    @asset_check(asset=asset2)
    def check2():
        return AssetCheckResult(success=False)

    defs = Definitions(
        assets=[asset1, asset2],
        asset_checks=[check1, check2],
        jobs=[define_asset_job("job1", selection=[asset1])],
    )
    job_def = defs.get_job_def("job1")
    result = job_def.execute_in_process()
    assert result.success

    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 1
    check_eval = check_evals[0]
    assert check_eval.asset_key == asset1.key
    assert check_eval.check_name == "check1"


def test_asset_not_provided():
    with pytest.raises(
        DagsterInvalidDefinitionError, match="No target asset provided when defining check 'check1'"
    ):

        @asset_check(description="desc")
        def check1():
            ...


def test_managed_input():
    @asset
    def asset1() -> int:
        return 4

    @asset_check(description="desc")
    def check1(asset1):
        assert asset1 == 4
        return AssetCheckResult(success=True)

    class MyIOManager(IOManager):
        def load_input(self, context):
            assert context.asset_key == asset1.key
            return 4

        def handle_output(self, context, obj):
            ...

    assert check1.name == "check1"
    assert check1.asset_key == asset1.key

    assert execute_assets_and_checks(
        assets=[asset1], asset_checks=[check1], resources={"io_manager": MyIOManager()}
    ).success


def test_multiple_managed_inputs():
    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=re.escape(
            "When defining check 'check1', Multiple target assets provided: ['asset1', 'asset2']."
            " Only one is allowed."
        ),
    ):

        @asset_check(description="desc")
        def check1(asset1, asset2):
            ...


def test_managed_input_with_context():
    @asset
    def asset1() -> int:
        return 4

    @asset_check(description="desc")
    def check1(context, asset1):
        assert context
        assert asset1 == 4
        return AssetCheckResult(success=True)

    assert check1.name == "check1"
    assert check1.asset_key == asset1.key

    execute_assets_and_checks(assets=[asset1], asset_checks=[check1])
