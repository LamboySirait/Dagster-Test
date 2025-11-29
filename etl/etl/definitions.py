from dagster import Definitions, load_assets_from_modules
from . import assets

# Load all assets from the assets.py file
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
)