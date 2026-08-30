# data/

Trimmed from upstream jobseek (`apps/crawler/data/`). Only two files are used
by this fork's pipeline (`src/pgpipe/`):

| file | rows | schema |
|---|---|---|
| `companies.csv` | ~500 | `slug,name,website,logo_url,icon_url,logo_type,industry,employee_count_range,founded_year,extras` (upstream schema, unchanged) |
| `boards.csv` | ~516 | `company_slug,board_slug,board_url,monitor_type,monitor_config,scraper_type,scraper_config` (upstream schema; `scraper_*` columns are ignored — this fork uses rich monitors only) |

`jobs sync` loads `slug,name,website,industry` into `crawler.company` and
`company_slug,board_slug,board_url,monitor_type,monitor_config` into
`crawler.job_board`. Everything else is ignored.

Regenerate from a fresh upstream clone with `tools/trim_csvs.py`
(keeps only companies whose every board uses a rich, non-browser HTTP monitor).

Upstream's taxonomy CSVs (`industries.csv`, `occupations.csv`, `seniority.csv`,
`technologies.csv`, `company_descriptions.csv`) and `images/` were **removed** —
this fork does no taxonomy resolution and stores no descriptions.
