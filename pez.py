from datetime import datetime
import requests


ALLOWED_NUMRE = {
    "1B.", "2B.", "3B.", "4B.", "5B.", "7B.",
    "8B.", "9B.", "10B.", "12B.", "19B.", "23B."
}

FAKTURALINJE_MAP = {
    "10B.": "751_Materiel pr. kvadratmeter",
    "12B.": "751_Materiel pr. kvadratmeter",
    "19B.": "751_Byggeplads pr.kvadratmeter",
    "1B.":  "751_Afmærkning pr.kvadratmeter",
    "23B.": "751_Bygninger pr. kvadratmeter",
    "2B.":  "751_Afmærkning pr.kvadratmeter",
    "3B.":  "751_Materiel pr. kvadratmeter",
    "4B.":  "751_Lift pr. kvadratmeter",
    "5B.":  "751_Kran pr. kvadratmeter",
    "7B.":  "751_Skurvogn pr. kvadratmeter",
    "8B.":  "751_Container pr. kvadratmeter",
    "9B.":  "751_Stillads pr. kvadratmeter",
}


def fetch_pez_cases(orchestrator_connection, session: requests.Session, username: str, password: str):
    access_token = login(session, username, password)

    headers_list = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {access_token}",
        "referer": "https://pez.giantleap.net/cases",
        "x-bpid": "bp_aarhus",
        "x-gltlocale": "da",
    }

    headers_detail = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {access_token}",
        "priority": "u=1, i",
        "x-bpid": "bp_aarhus",
        "x-gltlocale": "da",
    }

    params = {
        "case_type": "PARKING_TICKET",
        "contract": "35e4ac45-f820-48a5-b156-ae24b76e4ae4",
        "current_step": "2a8e5c34-ed88-4c9c-9d3b-a6af1013b3c1",
        "limit": 50,
        "offset": 0,
        "q": "",
        "sort_column": "date",
        "sort_direction": "DESC",
    }

    all_results = []
    while True:
        response = session.get(
            "https://pez.giantleap.net/rest/tickets/cases",
            headers=headers_list,
            params=params,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()

        all_results.extend(data.get("results", []))
        if not data.get("hasMore"):
            break

        params["offset"] += params["limit"]

    cases = []

    for case in all_results:
        case_uuid = case.get("id")
        if not case_uuid:
            continue

        detail_resp = session.get(
            f"https://pez.giantleap.net/rest/tickets/cases/{case_uuid}",
            headers=headers_detail,
            timeout=60
        )
        if detail_resp.status_code != 200:
            continue

        cjson = detail_resp.json().get("result") or {}
        henstilling_id = (str(cjson.get("number")).strip() if cjson.get("number") else None)
        if not henstilling_id:
            continue

        ticket = cjson.get("parkingTicket") or {}
        street_location = ticket.get("streetLocation") or {}

        streetname = (street_location.get("streetName") or "").strip()
        housenumber = (street_location.get("houseNumber") or "").strip()
        locationby = (ticket.get("locationBy") or "").strip()

        if not streetname:
            continue

        full_address = streetname
        if housenumber:
            full_address = f"{full_address} {housenumber}"
        if locationby:
            full_address = f"{full_address} - {locationby}"

        from_time_raw = ticket.get("fromTime")
        start_date = None
        if from_time_raw:
            start_date = datetime.strptime(from_time_raw, "%Y-%m-%d %H:%M:%S").date().isoformat()

        violations_raw = {
            1: ticket.get("violation1Name"),
            2: ticket.get("violation2Name"),
            3: ticket.get("violation3Name"),
        }

        forseelser = []
        for nummer, value in violations_raw.items():
            if not value or not isinstance(value, str):
                continue

            tokens = value.strip().split(maxsplit=1)
            if not tokens:
                continue

            code = tokens[0]
            if code in ALLOWED_NUMRE:
                forseelser.append({
                    "nummer": nummer,
                    "text": value.strip(),
                    "tilladelsestype": FAKTURALINJE_MAP.get(code),
                })

        if not forseelser:
            continue

        owner_resp = session.get(
            f"https://pez.giantleap.net/rest/tickets/cases/{case_uuid}/vehicle-owners",
            headers=headers_detail,
            timeout=60
        )
        if owner_resp.status_code != 200:
            continue

        owner = owner_resp.json().get("result") or {}
        cvr = owner.get("identificationNumber")
        if not cvr or not isinstance(cvr, str):
            continue

        cvr = cvr.strip()
        if not is_valid_cvr(cvr):
            continue

        coords = ticket.get("coordinates") or {}
        source_lat = to_float(coords.get("latitude"))
        source_lon = to_float(coords.get("longitude"))

        cases.append({
            "case_uuid": case_uuid,
            "henstilling_id": henstilling_id,
            "forseelser": forseelser,
            "cvr": cvr,
            "company_name": (owner.get("name") or "").strip() or None,
            "full_address": full_address,
            "source_lat": source_lat,
            "source_lon": source_lon,
            "start_date": start_date,
        })

    return cases, access_token


def add_sent_to_tilsyn_comment(session: requests.Session, access_token: str, case_uuid: str, forseelser: list[dict]) -> None:
    parts = []
    for f in sorted(forseelser, key=lambda x: x.get("nummer", 0)):
        text = (f.get("text") or "").strip()
        if text and not text.endswith("."):
            text += "."
        parts.append(f"Afvigelse {f['nummer']}: {text}")

    payload = {
        "comment": f"Sendt til AAK Tilsyn -> {' | '.join(parts)}",
        "isInternal": True,
    }

    r = session.post(
        f"https://pez.giantleap.net/rest/tickets/cases/{case_uuid}/comments",
        headers={
            "accept": "application/json, text/plain, */*",
            "authorization": f"Bearer {access_token}",
            "content-type": "application/json;charset=UTF-8",
            "priority": "u=1, i",
            "x-bpid": "bp_aarhus",
            "x-gltlocale": "da",
        },
        json=payload,
        timeout=30,
    )
    r.raise_for_status()


def login(session: requests.Session, username: str, password: str) -> str:
    session.get(
        "https://pez.giantleap.net/login",
        headers={"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
        timeout=30,
    ).raise_for_status()

    session.post(
        "https://pez.giantleap.net/rest/public/initiate-login",
        json={"username": username},
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://pez.giantleap.net",
            "referer": "https://pez.giantleap.net/login",
        },
        timeout=30,
    ).raise_for_status()

    resp = session.post(
        "https://pez.giantleap.net/rest/oauth/token",
        data={
            "client_id": "web-client",
            "grant_type": "password",
            "username": username,
            "password": password,
        },
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://pez.giantleap.net",
            "referer": "https://pez.giantleap.net/login",
            "authorization": "Basic d2ViLWNsaWVudDp3ZWItY2xpZW50",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def is_valid_cvr(cvr_str: str) -> bool:
    if len(cvr_str) != 8 or not cvr_str.isdigit():
        return False

    weights = [2, 7, 6, 5, 4, 3, 2, 1]
    total = sum(int(d) * w for d, w in zip(cvr_str, weights))
    return total % 11 == 0


def to_float(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None