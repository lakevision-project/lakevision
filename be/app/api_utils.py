import json
import numpy as np
import math
import decimal
import datetime as dt
import uuid
import pandas as pd
from typing import Any
from fastapi.responses import JSONResponse

def _clean_data_recursively(x: Any) -> Any:
    """
    Recursively traverses a data structure to replace special types and values
    that are not JSON serializable.
    """
    if isinstance(x, bytes):
        return '__binary_data__'
    if isinstance(x, (list, tuple, set)):
        return [_clean_data_recursively(v) for v in x]
    if isinstance(x, dict):
        return {k: _clean_data_recursively(v) for k, v in x.items()}

    # Handle numpy types
    if isinstance(x, np.integer):
        return int(x)
    if isinstance(x, np.floating):
        return None if (np.isnan(x) or np.isinf(x)) else float(x)
    if isinstance(x, np.bool_):
        return bool(x)
    if isinstance(x, np.ndarray):
        return x.tolist()

    # Handle standard float
    if isinstance(x, float):
        return None if (math.isnan(x) or math.isinf(x)) else x

    # Handle other common types
    if isinstance(x, decimal.Decimal):
        return float(x)
    if isinstance(x, (dt.datetime, dt.date, pd.Timestamp)):
        return x.isoformat()
    if isinstance(x, uuid.UUID):
        return str(x)

    return x

def df_to_records(df: pd.DataFrame) -> list[dict]:
    """
    Converts a DataFrame to a list of records, then cleans the data for JSON serialization.
    """
    records = df.to_dict(orient="records")
    return _clean_data_recursively(records)

class CleanJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        """
        Renders content to JSON after a cleaning pass to ensure serializability.
        """
        payload = _clean_data_recursively(content)
        return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")