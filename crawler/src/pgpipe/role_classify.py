"""Title classifier for the pool's role mix: 'tech' | 'product' | 'other'.

  tech     software / backend / frontend / full-stack / mobile, data / ML / AI,
           DevOps / SRE / platform / cloud, security, QA / test, embedded /
           firmware, infra / network, architects, engineering leadership.
  product  product-associated: product manager/owner, (technical) program /
           project manager, UX / UI / product design, product / business
           analyst, technical writer, solutions / sales / customer-success
           engineer.
  other    everything else (sales, HR, finance, ops, legal, ...). Dropped at
           ingest.

`classify_role(title, department)` -> (family, category) where family is one of
the above and category a finer label used by the analytics charts
(None for 'other'). Pure regex on the lowercase title (department is only a
tie-breaker for bare "Program/Project Manager"). No network, O(1).
"""
from __future__ import annotations

import re

FAMILIES = ("tech", "product", "other")


def _c(*pats: str) -> re.Pattern[str]:
    return re.compile("|".join(pats), re.I)


# --- product-associated roles that contain the word "engineer" (checked first,
# otherwise they'd be swallowed by the generic engineer rule) ---------------
_PRESALES = _c(
    r"\b(sales|solutions?|solution|pre-?sales|customer|technical sales) engineer",
    r"\bsales engineering\b", r"\bsolutions? engineering\b",
    r"\bpre-?sales (consultant|architect)",
)
_CS_ENG = _c(r"\bcustomer success engineer", r"\bsupport success engineer")
_TECH_WRITER = _c(r"\btechnical (writer|author|communicat)", r"\bdocumentation (engineer|writer|specialist)",
                  r"\bapi (technical )?writer", r"\bcontent designer\b")

# --- product roles that must NOT contain a hands-on engineer/developer word ----
_PM = _c(r"\bproduct (manager|owner|management|lead|director|operations manager)",
         r"\b(head|vp|vice president|director|chief|svp|avp|group) (of )?product\b",
         r"\bcpo\b", r"\bassociate product manager\b", r"\bapm\b",
         r"\bgroup product manager\b", r"\bproduct specialist\b")
_PROGRAM = _c(r"\b(technical|engineering|software|it|data|cloud|digital|platform|infrastructure|product|security|ai|ml) (program|project) (manager|lead|director|management)",
              r"\b(program|project) manager.{0,25}\b(technical|engineering|software|it|data|cloud|digital|platform|infrastructure|security|ai|ml)\b",
              r"\btpm\b", r"\bscrum master\b", r"\bdelivery manager\b", r"\bagile (coach|delivery)")
_PLAIN_PROGRAM = _c(r"\bprogram manager\b", r"\bproject manager\b")
_DESIGN = _c(r"\b(ux|ui|ui/ux|ux/ui|product|interaction|user experience|user interface|experience|service|visual product)\s*(/\s*\w+\s*)?(designer|design lead|design manager|researcher|design)\b",
             r"\bux (research|writer)", r"\bhead of design\b", r"\bdesign (lead|manager|director)\b",
             r"\b(principal|staff|senior|lead)\s+designer\b", r"\bproduct design\b", r"\buser research")
_ANALYST = _c(r"\bproduct analyst\b", r"\bbusiness analyst\b", r"\bbusiness systems analyst\b",
              r"\bproduct (data|insights?) analyst", r"\bbusiness analysis\b", r"\bfunctional analyst\b",
              r"\bsystems analyst\b", r"\bit business analyst\b")

# --- tech ----------------------------------------------------------------------
_ENG_WORD = re.compile(r"\b(engineer|developer|programmer|devops|sre|sde|swe|architect|scientist|sysadmin)\b", re.I)

_T_DATA = _c(r"\bdata (scien|engineer|analyst|architect|analytics|platform|infrastructure|ops|steward|modell?er)",
             r"\bmachine learning", r"\b(ml|ai|llm|nlp|genai|gen ai|mlops)\b", r"\bdeep learning",
             r"\bcomputer vision", r"\bgenerative ai", r"\bbi (developer|engineer|analyst)", r"\bbusiness intelligence",
             r"\banalytics engineer", r"\bapplied (scientist|researcher|research)", r"\bresearch (scientist|engineer)",
             r"\bdata\b.*\b(analyst|engineer|scientist)\b", r"\bquant(itative)? (developer|engineer|researcher|analyst)",
             r"\betl\b", r"\bdatabase (administrator|engineer|developer)", r"\bdba\b", r"\bdata ?bricks", r"\bsnowflake (developer|engineer)")
_T_DEVOPS = _c(r"\bdevops", r"\bsre\b", r"\bsite reliability", r"\bplatform (engineer|developer|architect|lead)",
               r"\bcloud\b", r"\binfrastructure\b", r"\breliability engineer", r"\bkubernetes", r"\bdevsecops",
               r"\b(aws|azure|gcp) (engineer|architect|administrator)", r"\bsystems? (administrator|engineer)", r"\bsysadmin",
               r"\brelease engineer", r"\bbuild (and|&) release", r"\bobservability")
_T_SEC = _c(r"\bsecurity\b", r"\bcyber", r"\binfosec", r"\bpenetration", r"\bpen ?test", r"\bappsec", r"\bsoc analyst",
            r"\bthreat (intel|detection|hunt|research)", r"\bvulnerab", r"\bred team", r"\bblue team",
            r"\bincident response", r"\bcryptograph", r"\bidentity (and|&) access", r"\biam (engineer|analyst|architect)",
            r"\bgrc\b", r"\bmalware")
_T_QA = _c(r"\bqa\b", r"\bsdet\b", r"\bquality assurance", r"\btest (engineer|automation|analyst|lead|manager)",
           r"\bautomation (engineer|tester|test)", r"\btester\b", r"\bsoftware test", r"\bperformance (test|engineer)",
           r"\bvalidation engineer", r"\bquality engineer.{0,20}(software|automation)", r"\bsoftware quality")
_T_EMBED = _c(r"\bembedded", r"\bfirmware", r"\bfpga\b", r"\basic\b", r"\bvlsi\b", r"\brtl\b", r"\bdesign verification",
              r"\bbsp\b", r"\bdevice driver", r"\brtos\b", r"\bsilicon (engineer|design|validation)", r"\bhardware (engineer|design|developer)",
              r"\bkernel", r"\bcompiler", r"\brobotics")
_T_NET = _c(r"\bnetwork (engineer|architect|administrator|security|automation|operations|specialist)", r"\bnoc (engineer|analyst)",
            r"\b(it|systems?|storage|virtualization|linux|windows|unix) (administrator|admin|engineer)", r"\bdatacenter",
            r"\bdata cent(er|re) (engineer|technician|operations)", r"\btelecom (engineer|software)", r"\b5g\b", r"\bradio (software|engineer)")
_T_MOBILE = _c(r"\bmobile\b", r"\bandroid\b", r"\bios\b", r"\bflutter\b", r"\breact native", r"\bswift(ui)?\b", r"\bkotlin\b")
_T_SWE = _c(r"\bsoftware\b", r"\bsde\b", r"\bswe\b", r"\bbackend|\bback[- ]end", r"\bfrontend|\bfront[- ]end",
            r"\bfull[- ]?stack", r"\bweb (developer|engineer)", r"\b(java|python|golang|go|node(\.?js)?|\.net|c\+\+|c#|rust|ruby|php|scala|react|angular|vue|typescript|javascript|kotlin|salesforce|sap abap|abap|servicenow|power ?bi|sql|oracle|dynamics|workday|mulesoft|tableau|ui) (developer|engineer|programmer|architect|consultant)",
            r"\bdeveloper\b", r"\bprogrammer\b", r"\bapplication (engineer|developer)", r"\bapi (engineer|developer)",
            r"\bintegration (engineer|developer)", r"\bgame (engineer|developer|programmer)", r"\bgraphics (engineer|programmer)",
            r"\bforward deployed", r"\bdeveloper (advocate|relations|experience)", r"\bblockchain", r"\bsmart contract",
            r"\bsolutions? architect", r"\benterprise architect", r"\btechnical architect", r"\bsoftware architect",
            r"\bsystems? architect", r"\bprincipal architect", r"\bplatform\b.*\bengineer", r"\bwebsite developer",
            r"\bsupport engineer", r"\btechnical support engineer", r"\bproduct engineer", r"\bsite engineer",
            r"\bresearch (software|engineer)", r"\bmember of technical staff", r"\bmts\b", r"\bsmts\b", r"\bpmts\b",
            r"\btechnical (lead|specialist|consultant|staff)", r"\btech lead\b", r"\blead engineer", r"\bprincipal engineer",
            r"\bstaff engineer", r"\bsenior engineer", r"\bdistinguished engineer", r"\bfellow engineer")
_T_LEAD = _c(r"\bengineering (manager|director|lead|head)", r"\b(director|head|vp|vice president|svp|avp) (of )?engineering",
             r"\bmanager,? (of )?engineering", r"\bchief (technology|technical|information security|data|ai) officer", r"\bcto\b",
             r"\bdevelopment manager", r"\bsoftware (development )?manager", r"\bengineering (program|delivery) lead")
# generic "engineer" that is NOT software/IT (civil, mechanical, ...)
_NON_SW_ENG = _c(r"\b(mechanical|civil|chemical|structural|manufacturing|process|industrial|hvac|plumbing|maintenance|petroleum|mining|"
                 r"geotechnical|environmental|biomedical|aerospace|aeronautical|automotive|electrical|electronics?|instrumentation|"
                 r"field service|field|service|installation|commissioning|construction|site|project|production|quality|reliability|"
                 r"safety|facilities|energy|power|plant|wireless|rf|optical|materials?|metallurgical|nuclear|marine|naval|drilling|"
                 r"reservoir|pipeline|cost|estimating|tooling|packaging|sales|application|test|validation|design|systems)\s+engineer",
                 r"\bengineer(ing)?\s*(technician|intern .*mechanical)", r"\bmechanical\b", r"\bcivil\b")

_INTERN_OTHER_BLOCK = _c(r"\b(sales|marketing|recruit|talent|human resources|\bhr\b|payroll|finance|financial|accountant|accounting|audit|"
                         r"legal|counsel|paralegal|supply chain|logistics|procurement|warehouse|driver|nurse|clinical|pharma|"
                         r"teacher|tutor|cashier|store|retail|barista|chef|cook|security guard|janitor|receptionist|"
                         r"customer (service|support|care) (representative|associate|agent|specialist)|account (executive|manager)|"
                         r"business development|operations manager|office manager|executive assistant|administrative)\b")


def _category_tech(t: str) -> str:
    if _T_SEC.search(t):
        return "Security"
    if _T_QA.search(t):
        return "QA / Test"
    if _T_DATA.search(t):
        return "Data / ML / AI"
    if _T_EMBED.search(t):
        return "Embedded / Firmware"
    if _T_DEVOPS.search(t):
        return "DevOps / SRE / Cloud"
    if _T_NET.search(t):
        return "Infra / Network"
    if re.search(r"\barchitect\b", t):
        return "Architect"
    if _T_LEAD.search(t):
        return "Engineering leadership"
    if _T_MOBILE.search(t):
        return "Mobile"
    return "Software engineering"


def _is_tech(t: str) -> bool:
    if _T_LEAD.search(t):
        return True
    strong = (_T_SEC, _T_QA, _T_DATA, _T_EMBED, _T_DEVOPS, _T_NET, _T_MOBILE, _T_SWE)
    has_eng = bool(_ENG_WORD.search(t))
    # bare-word signals ("security", "cloud", "ios", "data analyst") only count
    # when the title is also role-shaped, so "Security Guard"/"Cloud Sales" don't leak in
    for p in strong:
        if p.search(t):
            if p in (_T_SEC, _T_DEVOPS, _T_NET, _T_MOBILE, _T_EMBED) and not (
                has_eng or re.search(r"\b(analyst|administrator|specialist|lead|manager|researcher|consultant|expert|developer|tester)\b", t)
            ):
                continue
            if p is _T_SEC and re.search(r"\b(security (guard|officer|supervisor|attendant)|social security|securit(y|ies) (sales|trader|analyst, equit))", t):
                continue
            if _INTERN_OTHER_BLOCK.search(t) and not has_eng and p is not _T_DATA:
                continue
            return True
    # generic "<x> engineer": software/IT unless it is a physical-discipline engineer
    if re.search(r"\bengineer\b", t) and not _NON_SW_ENG.search(t):
        return True
    return False


_PHYSICAL = _c(r"\b(manufacturing|mechanical|civil|chemical|electrical|process|industrial|cnc|machinist|machining|construction|facilities|"
               r"site security|physical security|campus security|building security|value engineer\w*|clinical|laboratory|aquatic)\b")
_SOFT_HINT = _c(r"\b(software|data|cloud|developer|firmware|embedded|ai|ml|devops|cyber|it|digital|automation|erp|sap|network|information)\b")


def classify_role(title: str | None, department: str | None = None) -> tuple[str, str | None]:
    """(family, category). family in {'tech','product','other'}."""
    t = (title or "").strip().lower()
    if not t:
        return "other", None
    # 0. product marketing / sales ops etc. are corporate, never product-associated
    if re.search(r"\bproduct marketing\b|\bmarketing\b.*\bmanager\b|\bsales (operations|ops)\b", t) and not _ENG_WORD.search(t):
        return "other", None
    # 1. product-associated roles containing the word "engineer"
    if _CS_ENG.search(t):
        return "product", "Customer success eng."
    if _PRESALES.search(t):
        return "product", "Solutions / sales eng."
    if _TECH_WRITER.search(t):
        return "product", "Technical writing"
    hands_on = bool(re.search(r"\b(engineer|developer|programmer|sde|swe|devops|sre)\b", t))
    # 2. product roles (only when not a hands-on engineering title)
    if not hands_on:
        if _PM.search(t):
            return "product", "Product management"
        if _PROGRAM.search(t):
            return "product", "Program / project mgmt"
        if _DESIGN.search(t) and not re.search(r"\b(graphic|interior|fashion|floral|jewelry|landscape|game|hair|web design(er)? intern)\b", t):
            return "product", "Design / UX"
        if _ANALYST.search(t):
            return "product", "Product / business analysis"
        if _PLAIN_PROGRAM.search(t):
            dep = (department or "").lower()
            if re.search(r"engineer|technology|\bit\b|software|data|product|platform|infrastructure|security", dep):
                return "product", "Program / project mgmt"
    # 3. tech (physical-discipline titles like "Manufacturing Systems Engineer" are not)
    if _PHYSICAL.search(t) and not _SOFT_HINT.search(t):
        return "other", None
    if _is_tech(t):
        return "tech", _category_tech(t)
    return "other", None


def role_family(title: str | None, department: str | None = None) -> str:
    return classify_role(title, department)[0]
