"""tools/import_aja_boards.py: dedupe by (ats, token), idempotent, unsupported skipped."""
from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path

TOOL = Path(__file__).resolve().parents[3] / "tools" / "import_aja_boards.py"


def _load():
    spec = importlib.util.spec_from_file_location("import_aja_boards", TOOL)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_import_is_deduped_and_idempotent(tmp_path, monkeypatch, capsys):
    data = tmp_path / "data"
    data.mkdir()
    (data / "boards.csv").write_text(
        "company_slug,board_slug,board_url,monitor_type,monitor_config,scraper_type,scraper_config\n"
        'acme,acme-greenhouse,https://job-boards.greenhouse.io/acme,greenhouse,"{""token"": ""acme""}",skip,\n'
        "wd,wd-careers,https://nv.wd5.myworkdayjobs.com/Ext,workday,,skip,\n", encoding="utf-8")
    (data / "companies.csv").write_text(
        "slug,name,website,logo_url,icon_url,logo_type,industry,employee_count_range,founded_year,extras,company_type\n"
        "acme,Acme,,,,,,,,,product\nwd,WD,,,,,,,,,unknown\n", encoding="utf-8")
    aja = tmp_path / "c.json"
    aja.write_text(json.dumps({
        "greenhouse": ["acme", "Newco"], "lever": ["newco"], "ashby": ["newco"],
        "workday": [{"name": "nv", "host": "nv.wd5.myworkdayjobs.com", "site": "ext"},
                    {"name": "other", "host": "other.wd1.myworkdayjobs.com", "site": "Jobs"}],
        "icims": ["x.icims.com"], "successfactors": ["https://x/googlefeed.xml"],
    }), encoding="utf-8")
    mod = _load()
    for _ in range(2):
        monkeypatch.setattr(sys, "argv", ["x", "--aja", str(aja), "--data", str(data)])
        assert mod.main() == 0
    boards = list(csv.DictReader(open(data / "boards.csv", encoding="utf-8")))
    keys = [(b["monitor_type"], b["board_slug"]) for b in boards]
    assert len(keys) == len(set(keys)) == 6  # 2 existing + newco x3 + other
    assert ("workday", "other-workday") in keys and ("greenhouse", "newco-greenhouse") in keys
    cfg = json.loads(next(b for b in boards if b["board_slug"] == "other-workday")["monitor_config"])
    assert cfg == {"company": "other", "wd_instance": "wd1", "site": "Jobs"}
    companies = list(csv.DictReader(open(data / "companies.csv", encoding="utf-8")))
    assert sorted(c["slug"] for c in companies) == ["acme", "newco", "other", "wd"]
