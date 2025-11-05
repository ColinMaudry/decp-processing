import sys

from flows.decp_processing import decp_processing
from flows.get_cog import get_cog
from flows.sirene_preprocess import sirene_preprocess

FUNCTIONS = {
    "sirene_preprocess": sirene_preprocess,
    "decp_processing": decp_processing,
    "get_cog": get_cog,
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
    FUNCTIONS[func_name]()
