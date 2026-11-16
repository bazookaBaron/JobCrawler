# LinkedIn guest crawl on the Oracle VM

A systemd timer runs `python -m src.pgpipe.linkedin_search --window-hours 3` every 2 hours. It reads
LinkedIn's public, logged-out job search (guest API), filters to tech roles in US/IN/Remote, upserts into
`crawler.job_posting` (`source='linkedin'`) and then runs `SELECT crawler.build_feed()`.

It uses **systemd, not the GitHub Actions runner**, so it never occupies a runner slot and coexists with a
future AutoJobApplier self-hosted runner + Playwright on the same VM (the unit runs at `Nice=10`, idle I/O,
`CPUQuota=50%`, `MemoryMax=512M`, and installs only 4 small Python deps - no Playwright).

## Prerequisites

1. The crawler DB migration is applied (`jobs migrate`, from the `crawler-core` branch). The crawler checks for
   the new columns and `crawler.build_feed()` first and exits with code 2 and a clear message if they are missing.
2. A Postgres login for the crawler: `CREATE USER crawler_login PASSWORD '...' IN ROLE crawler_writer;`
   (never put a service-role key on this VM).
3. Ubuntu/Debian VM with outbound HTTPS (Oracle Always Free A1 / E2 micro both work).

## Install

```bash
git clone https://github.com/bazookaBaron/JobCrawler.git /tmp/jc && cd /tmp/jc/deploy/oracle-vm   # or any checkout
sudo REPO_BRANCH=main bash setup_linkedin_crawler.sh      # use REPO_BRANCH=feat/linkedin-guest-crawl until merged
sudo nano /etc/job-crawler/env                            # set CRAWLER_DATABASE_URL
```

The script is idempotent: re-run it to pull new code and refresh the units. It never overwrites an existing
`/etc/job-crawler/env` (mode 600, owner `jobcrawler`).

## Run once manually / logs

```bash
sudo systemctl start linkedin-crawl.service       # runs now (blocks until done, up to ~90 min budget)
journalctl -u linkedin-crawl -n 100 --no-pager    # last run
journalctl -u linkedin-crawl -f                   # follow
systemctl list-timers linkedin-crawl.timer        # next run
```

Each run ends with one line, e.g.
`SUMMARY requests=412 cards=1893 unique=640 kept=211 new=57 updated=154 reposted=6 filtered_out=429 {...} blocked=0 details=0 aborted=False`.

Dry-run without touching the DB (still makes real requests; keep `--max-requests` small):

```bash
cd /opt/job-crawler/crawler && sudo -u jobcrawler /opt/job-crawler/.venv/bin/python -m src.pgpipe.linkedin_search --dry-run --max-requests 5
```

Exit codes: `0` ok, `2` config/DB/schema problem, `3` aborted after 5 consecutive blocks (partial results are saved).

Options: `--window-hours`, `--max-requests` (default 700), `--queries-file` (default `crawler/data/linkedin_queries.json`:
35 keywords x 9 locations, 5 pages max), `--details` (also fetch the external apply URL for NEW jobs; off by default),
`--keep-other-countries`, `--title-exclude intern,trainee`. To change what is searched edit the JSON and re-run the setup script.

## Cost

Oracle Always Free: no extra cost. About 700 light HTTP requests every 2 h (one at a time, 2-4 s apart,
about 30-40 min per run), under 5% CPU and under 300 MB RAM on average, a few MB of egress per run.

## Risk: shared IP

The crawler and a future auto-apply bot would share this VM's public IP. If LinkedIn rate-limits or blocks the
IP (HTTP 429/999), the crawler backs off exponentially (max 4 retries) and aborts the run after 5 consecutive
blocks, then tries again at the next timer tick. Heavy or blocked guest traffic could in principle affect other
LinkedIn-facing tooling on the same IP, so keep the request budget modest. This crawler only ever uses the
logged-out guest endpoints: **never point it at a logged-in LinkedIn account, cookies or session tokens**.
Scraping may also be restricted by LinkedIn's terms; this is the user's call and risk.

## Uninstall

```bash
sudo systemctl disable --now linkedin-crawl.timer
sudo rm /etc/systemd/system/linkedin-crawl.{service,timer} && sudo systemctl daemon-reload
```
