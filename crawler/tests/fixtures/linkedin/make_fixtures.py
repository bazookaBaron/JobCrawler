"""Regenerates the minimal LinkedIn guest-API HTML fixtures in this directory.

    python tests/fixtures/linkedin/make_fixtures.py

Shapes mirror the public seeMoreJobPostings/search cards (no real data).
"""
from pathlib import Path

OUT = Path(__file__).parent


def card(jid, title, co, loc, tag):
    slug = co.lower().replace(" ", "-")
    tslug = title.lower().replace(" ", "-")
    return f"""<li>
  <div class="base-card relative w-full base-card--link base-search-card base-search-card--link job-search-card" data-entity-urn="urn:li:jobPosting:{jid}" data-search-id="x" data-impression-id="y">
    <a class="base-card__full-link absolute top-0 right-0 bottom-0 left-0 p-0 z-[2]" href="https://in.linkedin.com/jobs/view/{tslug}-at-{slug}-{jid}?position=1&amp;pageNum=0&amp;trk=public_jobs_jserp-result_search-card"><span class="sr-only">{title}</span></a>
    <div class="base-search-card__info">
      <h3 class="base-search-card__title">
        {title}
      </h3>
      <h4 class="base-search-card__subtitle">
        <a class="hidden-nested-link" href="https://www.linkedin.com/company/{slug}?trk=public_jobs_jserp-result_job-search-card-subtitle">
          {co}
        </a>
      </h4>
      <div class="base-search-card__metadata">
        <span class="job-search-card__location">
          {loc}
        </span>
        {tag}
      </div>
    </div>
  </div>
</li>
"""


def t_new(d, txt):
    return f'<time class="job-search-card__listdate--new" datetime="{d}">{txt}</time>'


def t_old(d, txt):
    return f'<time class="job-search-card__listdate" datetime="{d}">{txt}</time>'


cards = [
    card("4000000001", "Senior Software Engineer", "Acme Technologies Pvt Ltd", "Bengaluru, Karnataka, India", t_new("2026-11-16", "2 hours ago")),
    card("4000000002", "Backend Engineer", "Globex Corp", "Remote", t_new("2026-11-16", "45 minutes ago")),
    card("4000000003", "Data Scientist", "Initech", "San Francisco, CA", t_old("2026-11-14", "Reposted 3 hours ago")),
    card("4000000004", "Software Engineer II", "Umbrella Labs", "Hyderabad, Telangana, India", t_old("2026-11-13", "3 days ago")),
    card("4000000005", "Marketing Manager", "Hooli", "New York, NY", t_new("2026-11-16", "1 hour ago")),
    card("4000000006", "Software Engineer", "Euro GmbH", "Berlin, Germany", t_new("2026-11-16", "1 hour ago")),
    card("4000000007", "DevOps Engineer", "Pied Piper", "Pune, Maharashtra, India", t_old("2026-11-16", "Just now")),
    card("4000000008", "SDE Intern", "Stark Industries", "Mumbai, India", t_new("2026-11-16", "5 hours ago")),
    card("4000000009", "Platform Engineer", "Wayne Enterprises", "Austin, TX", t_old("2026-11-09", "1 week ago")),
    card("4000000010", "Full Stack Developer", "Cyberdyne", "Chennai, India", t_new("2026-11-16", "3 hours ago")),
]
(OUT / "search_page_full.html").write_text("\n".join(cards), encoding="utf-8")
(OUT / "search_page_short.html").write_text(cards[0] + cards[1], encoding="utf-8")
(OUT / "search_empty.html").write_text("\n<!-- no results -->\n", encoding="utf-8")
(OUT / "job_posting_external.html").write_text(
    '<html><body><div class="top-card-layout">\n'
    '<code id="applyUrl" style="display: none"><!--"https://www.linkedin.com/jobs/view/externalApply/4000000001'
    '?url=https%3A%2F%2Fboards.greenhouse.io%2Facme%2Fjobs%2F123%3Fgh_src%3Dli&amp;urlHash=abc'
    '&amp;trk=public_jobs_apply-link-offsite"--></code>\n</div></body></html>',
    encoding="utf-8",
)
(OUT / "job_posting_easy.html").write_text("<html><body><button>Easy Apply</button></body></html>", encoding="utf-8")
