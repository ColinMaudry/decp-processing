from tableschema import Table

from config import DIST_DIR


def validate_decp_against_tableschema():
    table = Table(
        str(DIST_DIR / "decp.csv"),
        schema="https://raw.githubusercontent.com/ColinMaudry/decp-table-schema/main/schema.json",
    )

    errors = []

    def exc_handler(exc, row_number=None, row_data=None, error_data=None):
        errors.append((exc.errors, f"row {row_number}", error_data))

    table.read(exc_handler=exc_handler)
    print(f"Erreurs de validation : {len(errors)}")
