# src/simple_pipeline.py
import argparse
import json
import os
import re
import csv
from pathlib import Path
from typing import Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# ----------------------------
# Normalización de RaceID
# ----------------------------
_RACEID_WORDS = re.compile(r"[A-Za-z]+")
_RACEID_DIGITS = re.compile(r"\d+")


def standardize_race_id(val: str) -> str:
    """
    Estandariza RaceID al formato <string><numero>, con la parte string en minúsculas.
    Ejemplos:
      "Cup 25"      -> "cup25"
      "league:04"   -> "league04"
      "race_11"     -> "race11"
    """
    if not isinstance(val, str):
        return val
    text = val.strip()
    word = "".join(_RACEID_WORDS.findall(text)).lower()
    digits = "".join(_RACEID_DIGITS.findall(text))
    if word and digits:
        return f"{word}{digits}"
    return re.sub(r"[^0-9a-zA-Z]", "", text).lower()


# ----------------------------
# Utilidades país / CSV
# ----------------------------
def _norm_country_key(name: str) -> str:
    """Normaliza el nombre del país antes de usarlo como clave de búsqueda."""
    return (name or "").strip().lower()


ALIAS = {
    "usa": "united states",
    "us": "united states",
    "u.s.": "united states",
    "uk": "united kingdom",
    "uae": "united arab emirates",
}


def build_country_lut(csv_path: str):
    """
    Lee un CSV con datos de países y genera un diccionario (lut) donde la clave
    es el país normalizado y el valor contiene:
      {country, capital, continent, official language, currency}

    Robusto a:
      - Delimitadores: coma, punto y coma, tab, pipe.
      - BOM en UTF-8.
      - Encabezados con espacios/guiones/underscores o nombres alternativos.
    """
    FIELD_ALIASES = {
        "country": ["Country", "Country Name", "Country_Name", "CountryName", "Name"],
        "capital": ["Capital", "Capital City", "Capital_City", "CapitalCity"],
        "continent": ["Continent", "Continent Name", "Region", "Subregion"],
        "official language": [
            "Main Official Language", "Main_Official_Language",
            "Official Language", "Official_Language",
            "Language", "Languages",
        ],
        "currency": [
            "Currency", "Currency Code", "Currency_Code",
            "Currency Name", "Currency_Name", "CurrencyName", "ISO Currency",
        ],
    }

    def _norm_header(h: str) -> str:
        return re.sub(r"[\s_\-]+", "", (h or "").strip().lower())

    lut = {}

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t,")
        except Exception:
            dialect = csv.excel  # por defecto: coma
        reader = csv.DictReader(f, dialect=dialect)

        header_norm_map = {_norm_header(h): h for h in (reader.fieldnames or [])}

        def get_value(row, candidates):
            for cand in candidates:
                key = header_norm_map.get(_norm_header(cand))
                if key and key in row:
                    val = row.get(key)
                    if val is not None:
                        sval = str(val).strip()
                        if sval:
                            return sval
            return ""

        for row in reader:
            src_country = get_value(row, FIELD_ALIASES["country"])
            if not src_country:
                continue

            payload = {
                "country": get_value(row, FIELD_ALIASES["country"]),
                "capital": get_value(row, FIELD_ALIASES["capital"]),
                "continent": get_value(row, FIELD_ALIASES["continent"]),
                "official language": get_value(row, FIELD_ALIASES["official language"]),
                "currency": get_value(row, FIELD_ALIASES["currency"]),
            }

            key_norm = _norm_country_key(src_country)
            lut[key_norm] = payload

    # Crea claves alias si existe el país canónico
    for alias, canonical in ALIAS.items():
        if canonical in lut:
            lut[alias] = lut[canonical]

    return lut


# ----------------------------
# Transformaciones Beam
# ----------------------------
class ParseJson(beam.DoFn):
    """Convierte líneas JSON en dicts Python."""
    def process(self, line):
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj
        except Exception:
            return  # Ignora líneas inválidas


class FilterDeviceOther(beam.DoFn):
    """Filtra DeviceType == 'Other'."""
    def process(self, row):
        if str(row.get("DeviceType", "")).strip() != "Other":
            yield row


class StandardizeRace(beam.DoFn):
    """Normaliza RaceID."""
    def process(self, row):
        row = dict(row)
        row["RaceID"] = standardize_race_id(row.get("RaceID", ""))
        yield row


class EnrichWithCountry(beam.DoFn):
    """Enriquece usando LUT del CSV y genera LocationData anidado."""
    def __init__(self, lut):
        self.lut = lut

    def process(self, row):
        row = dict(row)
        country_raw = row.pop("ViewerLocationCountry", None)
        key = _norm_country_key(country_raw or "")
        key = ALIAS.get(key, key)
        payload = self.lut.get(key)
        if not payload:
            payload = {
                "country": (country_raw or "").strip(),
                "capital": "",
                "continent": "",
                "official language": "",
                "currency": "",
            }
        row["LocationData"] = payload
        yield row


class ProjectOutputSchema(beam.DoFn):
    """Proyecta al esquema final y serializa a JSON (una línea por registro)."""
    KEEP = [
        "FanID",
        "RaceID",
        "Timestamp",
        "DeviceType",
        "EngagementMetric_secondswatched",
        "PredictionClicked",
        "MerchandisingClicked",
        "LocationData",
    ]

    def process(self, row):
        out = {k: row.get(k) for k in self.KEEP}
        yield json.dumps(out, ensure_ascii=False)


# ----------------------------
# CLI / Paths
# ----------------------------
def parse_args(argv=None):
    """
    Define y parsea argumentos de línea de comandos.
    Cumple rúbrica: parámetros para paths de entrada y salida.
    """
    parser = argparse.ArgumentParser(
        description="Tarea 1 - Módulo 8: Beam pipeline (JSON + CSV -> JSONL)"
    )
    parser.add_argument("--runner", default="DirectRunner", help="Runner de Beam")
    # Paths de ENTRADA (cumplimiento rúbrica)
    parser.add_argument(
        "--input_json_glob",
        default=None,
        help='Patrón (glob) de JSONs de entrada. Ej: "input/*fan_engagement-000-of-001.json"',
    )
    parser.add_argument(
        "--input_csv",
        default=None,
        help='Ruta al CSV de países. Ej: "input_side/country_data_v2.csv"',
    )
    # Salida
    parser.add_argument("--output_folder", default="output", help="Carpeta de salida relativa a la raíz del proyecto")
    parser.add_argument("--output_prefix", default="result", help="Prefijo del archivo de salida (sin extensión)")
    return parser.parse_args(argv)


def resolve_paths(
    output_folder: str,
    input_json_glob_cli: Optional[str],
    input_csv_cli: Optional[str],
):
    """
    Resuelve rutas relativas a la raíz del repo, permitiendo override por CLI.
    """
    root = Path(__file__).resolve().parents[1]

    input_json_glob = input_json_glob_cli or str(root / "input" / "*fan_engagement-000-of-001.json")

    if input_csv_cli:
        input_csv = input_csv_cli
    else:
        csv_candidates = [
            root / "input_side" / "country_data_v2.csv",
            root / "input side" / "country_data_v2.csv",
        ]
        input_csv = next((p for p in csv_candidates if p.exists()), csv_candidates[0])

    output_dir = root / output_folder
    output_dir.mkdir(parents=True, exist_ok=True)

    return input_json_glob, str(input_csv), str(output_dir)


# ----------------------------
# Main / Runner
# ----------------------------
def run(argv=None):
    """Ejecución principal del pipeline Apache Beam."""
    args = parse_args(argv)
    input_json_glob, input_csv, output_dir = resolve_paths(
        args.output_folder, args.input_json_glob, args.input_csv
    )
    out_prefix = os.path.join(output_dir, args.output_prefix)

    country_lut = build_country_lut(input_csv)

    options = PipelineOptions(
        [
            "--runner", args.runner,
            "--save_main_session", "True",
        ]
    )

    with beam.Pipeline(options=options) as p:
        lines = p | "ReadJSONLs" >> beam.io.ReadFromText(file_pattern=input_json_glob)

        parsed = lines | "ParseJson" >> beam.ParDo(ParseJson())

        filtered = parsed | "FilterDeviceOther" >> beam.ParDo(FilterDeviceOther())

        standardized = filtered | "StandardizeRace" >> beam.ParDo(StandardizeRace())

        enriched = standardized | "EnrichWithCountry" >> beam.ParDo(EnrichWithCountry(country_lut))

        projected = enriched | "ProjectOutputSchema" >> beam.ParDo(ProjectOutputSchema())

        _ = projected | "WriteJSONL" >> beam.io.WriteToText(
            file_path_prefix=out_prefix,
            file_name_suffix=".jsonl",
            num_shards=1,
        )


if __name__ == "__main__":
    run()
