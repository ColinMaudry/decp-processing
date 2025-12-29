import sys

from src.config import SCRAPING_MODE, SCRAPING_MONTH, SCRAPING_TARGET, SCRAPING_YEAR
from src.flows.decp_processing import decp_processing
from src.flows.get_cog import get_cog
from src.flows.scrap import scrap
from src.flows.sirene_preprocess import sirene_preprocess

FUNCTIONS = {
    "sirene_preprocess": sirene_preprocess,
    "decp_processing": decp_processing,
    "get_cog": get_cog,
    "scrap": scrap,
}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python flows.py <function_name>")
        print("Available functions:", list(FUNCTIONS.keys()))
        sys.exit(1)

    func_name = sys.argv[1]
    if func_name not in FUNCTIONS:
        print(f"Unknown function: {func_name}")
        print("Available functions:", list(FUNCTIONS.keys()))
        sys.exit(1)

    # Call the function
    if func_name != "scrap":
        FUNCTIONS[func_name]()
    else:
        scrap(
            mode=SCRAPING_MODE,
            target=SCRAPING_TARGET,
            month=SCRAPING_MONTH,
            year=SCRAPING_YEAR,
        )
