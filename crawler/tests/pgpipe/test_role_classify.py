import pytest

from src.pgpipe.role_classify import classify_role


@pytest.mark.parametrize("title,fam", [
    ("Senior Software Engineer", "tech"), ("Backend Developer", "tech"), ("Staff Frontend Engineer", "tech"),
    ("Android Developer", "tech"), ("Data Scientist II", "tech"), ("Machine Learning Engineer", "tech"),
    ("Site Reliability Engineer", "tech"), ("DevOps Engineer", "tech"), ("Cloud Platform Engineer", "tech"),
    ("Security Engineer", "tech"), ("Cyber Security Analyst", "tech"), ("QA Engineer", "tech"),
    ("SDET", "tech"), ("Embedded Firmware Engineer", "tech"), ("Network Engineer", "tech"),
    ("Solutions Architect", "tech"), ("Engineering Manager", "tech"), ("Director of Engineering", "tech"),
    ("Software Engineering Intern", "tech"), ("Principal Engineer", "tech"), ("Data Engineer", "tech"),
    ("Product Security Engineer", "tech"), ("Member of Technical Staff", "tech"),
    ("Product Manager", "product"), ("Senior Product Owner", "product"), ("Technical Program Manager", "product"),
    ("Product Manager, Machine Learning", "product"), ("UX Designer", "product"), ("Product Designer", "product"),
    ("Business Analyst", "product"), ("Technical Writer", "product"), ("Solutions Engineer", "product"),
    ("Sales Engineer", "product"), ("Customer Success Engineer", "product"), ("Engineering Program Manager", "product"),
    ("Account Executive", "other"), ("Sales Manager", "other"), ("Recruiter", "other"), ("Financial Analyst", "other"),
    ("HR Business Partner", "other"), ("Product Marketing Manager", "other"), ("Mechanical Engineer", "other"),
    ("Civil Engineer", "other"), ("Security Guard", "other"), ("Accountant", "other"), ("Marketing Manager", "other"),
    ("Technical Account Manager", "other"), ("Supply Chain Manager", "other"), ("Graphic Designer", "other"),
    ("", "other"),
])
def test_classify(title, fam):
    assert classify_role(title)[0] == fam, classify_role(title)


def test_mix_caps_and_gate():
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace

    from src.pgpipe import cleanup
    from src.pgpipe.cleanup import MixGate, mix_caps

    # 10% shares: product <= tech/9, US <= india/9
    assert mix_caps(900, 900) == (100, 100)
    assert mix_caps(0, 0) == (0, 0)

    g = MixGate(None)
    newer = datetime.now(timezone.utc)
    older = newer - timedelta(days=5)
    g._bound = {"product": newer - timedelta(days=1), "US": "all"}
    assert g.allow("tech", "IN", older)              # uncapped class always passes
    assert not g.allow("product", "IN", older)       # at cap and older than the kept rows
    assert g.allow("product", "IN", newer)           # newer than boundary
    assert not g.allow("tech", "US", newer)          # US cap is 0
