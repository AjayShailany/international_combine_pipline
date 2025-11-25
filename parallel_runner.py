# utils/parallel_runner.py
import importlib
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any

COUNTRIES_DIR = pathlib.Path(__file__).parent.parent / "countries"

def _load_country_module(country_file: pathlib.Path):
    module_name = f"scraper_pipeline.countries.{country_file.stem}"
    return importlib.import_module(module_name)

def run_countries_parallel(max_workers: int = None) -> Dict[str, Any]:
    country_files = [p for p in COUNTRIES_DIR.glob("*.py") if p.name != "__init__.py"]
    modules = [_load_country_module(p) for p in country_files]

    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_country = {
            executor.submit(getattr(mod, "run", lambda: None)): mod.__name__.split(".")[-1]
            for mod in modules
        }
        for future in as_completed(future_to_country):
            country = future_to_country[future]
            try:
                results[country] = future.result()
            except Exception as exc:
                results[country] = {"error": repr(exc)}
    return results