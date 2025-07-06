import pandas as pd
from io import BytesIO
import json
import xml.etree.ElementTree as ET

def load_file(file_name: str, file_bytes: bytes) -> pd.DataFrame:
    try:
        if file_name.endswith(".csv"):
            df = pd.read_csv(BytesIO(file_bytes))

        elif file_name.endswith(".tsv"):
            df = pd.read_csv(BytesIO(file_bytes), sep="\t")

        elif file_name.endswith(".xlsx"):
            df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")

        elif file_name.endswith(".xls"):
            df = pd.read_excel(BytesIO(file_bytes))

        elif file_name.endswith(".ods"):
            import odf
            df = pd.read_excel(BytesIO(file_bytes), engine="odf")

        elif file_name.endswith(".parquet"):
            import pyarrow.parquet as pq
            table = pq.read_table(BytesIO(file_bytes))
            df = table.to_pandas()

        elif file_name.endswith(".feather"):
            import pyarrow.feather as feather
            df = feather.read_feather(BytesIO(file_bytes))

        elif file_name.endswith(".orc"):
            raise NotImplementedError("ORC format is not supported yet. Use parquet instead.")

        elif file_name.endswith(".json"):
            json_data = json.loads(file_bytes.decode("utf-8"))
            df = pd.json_normalize(json_data)

        elif file_name.endswith(".xml"):
            import xmltodict
            parsed = xmltodict.parse(file_bytes.decode("utf-8"))
            df = pd.json_normalize(parsed)

        elif file_name.endswith(".yaml") or file_name.endswith(".yml"):
            from ruamel.yaml import YAML
            yaml = YAML()
            parsed = yaml.load(BytesIO(file_bytes))
            df = pd.json_normalize(parsed)

        else:
            raise ValueError(f"Unsupported file format: {file_name}")

        if df.empty:
            raise ValueError("Uploaded file is empty.")

        return df

    except Exception as e:
        raise ValueError(f"Error while loading file: {e}")
