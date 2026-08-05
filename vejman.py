from datetime import datetime, timedelta
import re

from pyproj import Transformer


ALLOWED_INITIALS = {"MAMASA", "LERV", "MABMO", "JKROG", "MOJUS"}
EPSG25832_TO_WGS84 = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)

# The middle value of each entry in the urls list below: whether a case seen only
# in that window may be skipped when nothing in its list payload changed.
FULL_REFETCH = False   # narrow windows — every case refetched in full, every run
REFRESH_ONLY = True    # wide window — too many cases to pull details for daily

CASE_FIELDS = "state%2Ctype%2Ccase_id%2Ccase_number%2Cauthority_reference_number%2Cmarker%2Cwebgtno%2Cstart_date%2Cend_date%2Ccompletion_date%2Capplicant_folder_number%2Cconnected_case%2Cstreet_name%2Capplicant%2Crovm_equipment_type%2Cinitials"

# The list-level fields we ask Vejman for. Hashing exactly these lets process.py
# tell whether anything about a case moved without pulling the case details.
# completion_date matters here: the day a case is færdigmeldt, both state and
# completion_date change, so the case can never be skipped as unchanged.
#
# Note the granularity: getcases returns dates without a time, so an edit that
# only moves the hours within the same day (slutdato 15-10 16:00 -> 15-10 20:00)
# leaves this hash unchanged and the case is skipped. That is not a permanent
# miss — on its slutdato the case drops out of the wide window and into
# "Udløbne tilladelser", which always refetches in full, so the time is correct
# again on the day it matters.
CASE_FINGERPRINT_FIELDS = (
    "state", "type", "case_id", "case_number", "authority_reference_number",
    "marker", "webgtno", "start_date", "end_date", "completion_date",
    "applicant_folder_number", "connected_case", "street_name", "applicant",
    "rovm_equipment_type", "initials",
)


def fetch_vejman_case_list(orchestrator_connection, session, vejman_token: str):
    """Return the base (list-level) Vejman cases we care about today.

    Four list calls, no per-case calls. Three narrow windows catch the daily
    churn — udløbne, færdigmeldte and nye tilladelser — and every case on those
    is refetched in full every run, as it always was.

    The fourth is wide: every tilladelse whose slutdato is still ahead of us but
    which sits in the gap between those windows. It exists because such a case is
    on none of the three lists, so nothing else would ever look at it: it
    backfills the ones that were never added while they were nye, and it catches
    ændrede datoer, which move a case out of the narrow windows and would
    otherwise leave the stored dates stale.

    It is not filtered on start date, because a startdato can be moved forward
    after the sag has begun. Cases it returns that have not begun yet carry
    not_yet_started=True; process.py refreshes those only if they are already in
    the app and otherwise leaves them to the "nye" window.

    That wide window returns every active tilladelse, which is far too many to
    pull details for on every run, so its cases are flagged refresh_only=True.
    process.py fingerprints their list fields and only pays for the per-case
    call when a case is new or something in the list moved. Those list dates
    have no time of day, so a forlængelse of a few hours within the same day is
    not detected here — the case is refetched in full on its slutdato by the
    udløbne window anyway.

    A case that shows up in both a narrow and the wide window keeps full
    treatment: refresh_only is only True when the wide window is the sole match.
    """
    now = datetime.now()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    today = now.strftime("%Y-%m-%d")
    tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")

    urls = [
        (
            "Udløbne tilladelser",
            FULL_REFETCH,
            f"https://vejman.vd.dk/permissions/getcases"
            f"?pmCaseStates=3"
            f"&pmCaseFields={CASE_FIELDS}"
            f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
            f"&pmCaseShowAttachments=false"
            f"&endDateFrom={yesterday}&endDateTo={today}"
            f"&token={vejman_token}"
        ),
        (
            "Færdigmeldte tilladelser",
            FULL_REFETCH,
            f"https://vejman.vd.dk/permissions/getcases"
            f"?pmCaseStates=8"
            f"&pmCaseFields={CASE_FIELDS}"
            f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
            f"&pmCaseShowAttachments=false"
            f"&endDateFrom={yesterday}&endDateTo={today}"
            f"&token={vejman_token}"
        ),
        (
            "Nye tilladelser",
            FULL_REFETCH,
            f"https://vejman.vd.dk/permissions/getcases"
            f"?pmCaseStates=3%2C6%2C8%2C12"
            f"&pmCaseFields={CASE_FIELDS}"
            f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
            f"&pmCaseShowAttachments=false"
            f"&startDateFrom={today}&startDateTo={today}"
            f"&token={vejman_token}"
        ),
        (
            # Slutdato after today. Cases ending today are already covered above,
            # so the bound is exclusive.
            #
            # Deliberately unfiltered on start date. A godkendt sag can have its
            # startdato moved forward even after it has begun (seen 16-06-2026:
            # start 08-06 -> 27-07, slut 10-07 -> 28-08), which would drop it out
            # of a startDateTo bound for weeks — exactly while the stored dates are
            # wrong. So we take every case ending after today and let process.py
            # decide: already in the app means refresh it, not in the app and not
            # yet begun means leave it for the "nye" window on its start date.
            #
            # Two jobs. It backfills cases that began before the robot started
            # running (april 2026) and were therefore never picked up. And it keeps
            # the ones we already hold in sync — a forlængelse pushes end_date out
            # of the narrow windows above, so without this the stored end_date goes
            # stale and the case looks expired in the app.
            #
            # State 3 (godkendt) is where date changes happen: udløbne og
            # færdigmeldte sager kan ikke forlænges (Mohamed Abdirisaq Ibrahim,
            # 04-08-2026). State 8 is here for the opposite reason — a case
            # færdigmeldt long before its slutdato keeps that future slutdato, so
            # it appears in no other window until the original date passes. Its
            # completion_date is what we store as the real end.
            "Igangværende tilladelser",
            REFRESH_ONLY,
            f"https://vejman.vd.dk/permissions/getcases"
            f"?pmCaseStates=3%2C8"
            f"&pmCaseFields={CASE_FIELDS}"
            f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
            f"&pmCaseShowAttachments=false"
            f"&endDateFrom={tomorrow}"
            f"&token={vejman_token}"
        ),
    ]

    base_cases_by_number = {}

    for header, refresh_only, url in urls:
        orchestrator_connection.log_info(f"Fetching Vejman list: {header}")
        resp = session.get(url, timeout=60)
        resp.raise_for_status()

        cases = resp.json().get("cases", [])
        orchestrator_connection.log_info(f"{header}: {len(cases)} sag(er) før filtrering på initialer")

        for case in cases:
            if case.get("initials") not in ALLOWED_INITIALS:
                continue
            if not case.get("case_number"):
                continue
            if not case.get("case_id"):
                continue

            previous = base_cases_by_number.get(case["case_number"])
            # A case seen in any of the narrow windows keeps full treatment, no
            # matter which list it also shows up in.
            case["refresh_only"] = refresh_only and (previous is None or previous["refresh_only"])
            # Compared on the date alone — the list has no time of day, and a sag
            # starting later today is in the "nye" window anyway.
            start_date = parse_vejman_datetime(case.get("start_date")) or ""
            case["not_yet_started"] = start_date[:10] > today
            base_cases_by_number[case["case_number"]] = case

    return list(base_cases_by_number.values())


def list_level_fingerprint(case: dict) -> dict:
    """The list payload reduced to the fields we store, for change detection."""
    return {field: case.get(field) for field in CASE_FINGERPRINT_FIELDS}


def build_vejman_case(session, vejman_token: str, case: dict) -> dict:
    """Pull the case details and flatten list + details into one row."""
    case_id = case["case_id"]

    detail_resp = session.get(
        f"https://vejman.vd.dk/permissions/getcase?caseid={case_id}&token={vejman_token}",
        timeout=60,
    )
    detail_resp.raise_for_status()
    details = detail_resp.json().get("data", {}) or {}

    site = (details.get("sites") or [{}])[0] if details.get("sites") else {}
    building = site.get("building") or {}

    building_from = building.get("from")
    building_to = building.get("to")

    if building_from not in (None, "") and building_to not in (None, ""):
        house_text = str(building_from) if str(building_from) == str(building_to) else f"{building_from}-{building_to}"
    elif building_from not in (None, ""):
        house_text = str(building_from)
    elif building_to not in (None, ""):
        house_text = str(building_to)
    else:
        house_text = None

    street_name_raw = (case.get("street_name") or "").strip()
    full_address = f"{street_name_raw} {house_text}".strip() if street_name_raw and house_text else (street_name_raw or None)

    linestring = find_linestring_value(details) or find_linestring_value(case)
    source_lat = None
    source_lon = None

    if linestring:
        coord = extract_coord_from_linestring(linestring)
        if coord:
            source_lat, source_lon = coord

    # Details first, always: the getcases list carries the date only, getcase
    # carries date + time. The list values are just a fallback for a field the
    # details happen not to include.
    start_date = parse_vejman_datetime(details.get("start_date") or case.get("start_date"))
    slutdato = parse_vejman_datetime(details.get("end_date") or case.get("end_date"))
    completion_date = parse_vejman_datetime(details.get("completion_date") or case.get("completion_date"))

    return {
        "case_number": (case.get("case_number") or "").strip(),
        "case_id": str(case_id).strip(),
        "vejman_state": (case.get("state") or "").strip() or None,
        "connected_case": (case.get("connected_case") or "").strip() or None,
        "start_date": start_date,
        "end_date": effective_end_date(slutdato, completion_date),
        "completion_date": completion_date,
        "vejman_end_date": slutdato,
        "applicant": (case.get("applicant") or "").strip() or None,
        "marker": (case.get("marker") or "").strip() or None,
        "rovm_equipment_type": (case.get("rovm_equipment_type") or "").strip() or None,
        "applicant_folder_number": (case.get("applicant_folder_number") or "").strip() or None,
        "authority_reference_number": (case.get("authority_reference_number") or "").strip() or None,
        "street_status": (site.get("street_status") or "").strip() or None,
        "full_address": full_address,
        "street_name_raw": street_name_raw or None,  # unmodified, for SharePoint folder naming (must match dispatcher)
        "initials": (case.get("initials") or "").strip() or None,
        "source_lat": source_lat,
        "source_lon": source_lon,
    }


def effective_end_date(slutdato, completion_date):
    """When a case has been færdigmeldt, the completion date can only pull the
    end date in, never push it out.

    Færdigmeldt early: the area was released on the completion date, so that is
    the real end — this is what makes such a case stop looking active in the app.
    Færdigmeldt after the slutdato (a late registration): the permission still
    only ran to its slutdato, and udløbne/færdigmeldte sager kan ikke forlænges
    (Mohamed Abdirisaq Ibrahim, 04-08-2026), so a late completion date must not
    stretch the period.

    Both values are ISO strings, which sort chronologically.
    """
    if not completion_date:
        return slutdato
    if not slutdato:
        return completion_date
    return min(slutdato, completion_date)


def parse_vejman_datetime(value):
    """Vejman dates are dd-mm-yyyy. The getcases list only carries the date;
    getcase adds the time, which is why build_vejman_case prefers the details
    for every date field. Both forms are accepted here.

    Returns an ISO string, or None if the value is empty or unparseable — a
    single odd date must not take down the whole run."""
    if not value:
        return None

    text = str(value).strip()
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).isoformat()
        except ValueError:
            continue
    return None


def find_linestring_value(obj):
    if isinstance(obj, str):
        return obj if "LINESTRING" in obj.upper() else None

    if isinstance(obj, dict):
        value = obj.get("value")
        if isinstance(value, str) and "LINESTRING" in value.upper():
            return value
        for v in obj.values():
            found = find_linestring_value(v)
            if found:
                return found
        return None

    if isinstance(obj, list):
        for item in obj:
            found = find_linestring_value(item)
            if found:
                return found
        return None

    return None


def extract_coord_from_linestring(linestring: str):
    match = re.search(r"\(?([\d.]+)\s+([\d.]+)", linestring)
    if not match:
        return None

    east, north = map(float, match.groups())
    lon, lat = EPSG25832_TO_WGS84.transform(east, north)
    return lat, lon