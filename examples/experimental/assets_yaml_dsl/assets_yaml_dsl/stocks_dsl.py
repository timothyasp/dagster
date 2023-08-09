import os
from typing import Any, Dict, List, NamedTuple

import yaml
from dagster._core.definitions.events import Output

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from dagster import AssetKey, AssetOut, AssetsDefinition, asset, multi_asset


def load_yaml(relative_path: str) -> Dict[str, Any]:
    path = os.path.join(os.path.dirname(__file__), relative_path)
    with open(path, "r", encoding="utf8") as ff:
        return yaml.load(ff, Loader=Loader)


def get_ticker_data(ticker: str) -> str:
    # imagine instead of returning a string, this function fetches data from an external service
    return f"{ticker}-data"


def enrich_and_insert_data(ticker_data) -> None:
    # imagine this modifies the data and inserts it into ouj database
    pass


def fetch_data_for_ticker(ticker: str) -> str:
    # imagine this fetches data from our database
    return f"{ticker}-data-enriched"


class StockInfo(NamedTuple):
    ticker: str


class IndexStrategy(NamedTuple):
    type: str


class Forecast(NamedTuple):
    days: int


class StockAssets(NamedTuple):
    stock_infos: List[StockInfo]
    index_strategy: IndexStrategy
    forecast: Forecast


def build_stock_assets_object(stocks_dsl_document: Dict[str, Dict]) -> StockAssets:
    return StockAssets(
        stock_infos=[
            StockInfo(ticker=stock_block["ticker"])
            for stock_block in stocks_dsl_document["stocks_to_index"]
        ],
        index_strategy=IndexStrategy(type=stocks_dsl_document["index_strategy"]["type"]),
        forecast=Forecast(int(stocks_dsl_document["forecast"]["days"])),
    )


def get_stocks_dsl_example_defs() -> List[AssetsDefinition]:
    stocks_dsl_document = load_yaml("stocks.yaml")
    stock_assets = build_stock_assets_object(stocks_dsl_document)
    return assets_defs_from_stock_assets(stock_assets)


def assets_defs_from_stock_assets(stock_assets: StockAssets) -> List[AssetsDefinition]:
    group_name = "stocks"

    outs = {}
    tickers = []
    ticker_asset_keys = []
    for stock_info in stock_assets.stock_infos:
        ticker = stock_info.ticker
        ticker_asset_key = AssetKey(ticker)
        outs[ticker] = AssetOut(
            key=ticker_asset_key,
            description=f"Fetch {ticker} from internal service",
            group_name=group_name,
        )
        tickers.append(ticker)
        ticker_asset_keys.append(ticker_asset_key)

    @multi_asset(outs=outs)
    def fetch_the_tickers():
        for ticker in tickers:
            enrich_and_insert_data(get_ticker_data(ticker))

        # TODO: remove once alex's PR lands
        return tuple([Output(None, ticker) for ticker in tickers])

    @asset(deps=ticker_asset_keys, group_name=group_name)
    def index_strategy() -> None:
        stored_ticker_data = {}
        for ticker in tickers:
            stored_ticker_data[ticker] = fetch_data_for_ticker(ticker)

        # do someting with stored_ticker_data

    @asset(deps=ticker_asset_keys, group_name=group_name)
    def forecast() -> None:
        # do some forecast thing
        pass

    return [fetch_the_tickers, index_strategy, forecast]
