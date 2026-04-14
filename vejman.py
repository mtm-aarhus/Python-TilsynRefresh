from datetime import datetime, timedelta
import re

from pyproj import Transformer


ALLOWED_INITIALS = {"MAMASA", "LERV", "MABMO", "JKROG"}
EPSG25832_TO_WGS84 = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)


def fetch_vejman_cases(orchestrator_connection, session, vejman_token: str):
    now = datetime.now()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    today = now.strftime("%Y-%m-%d")

    urls = [
        (
            "Udløbne tilladelser",
            f"https://vejman.vd.dk/permissions/getcases"
            f"?pmCaseStates=3"
            f"&pmCaseFields=state%2Ctype%2Ccase_id%2Ccase_number%2Cauthority_reference_number%2Cmarker%2Cwebgtno%2Cstart_date%2Cend_date%2Capplicant_folder_number%2Cconnected_case%2Cstreet_name%2Capplicant%2Crovm_equipment_type%2Cinitials"
            f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
            f"&pmCaseShowAttachments=false"
            f"&endDateFrom={yesterday}&endDateTo={today}"
            f"&token={vejman_token}"
        ),
        (
            "Færdigmeldte tilladelser",
            f"https://vejman.vd.dk/permissions/getcases"
            f"?pmCaseStates=8"
            f"&pmCaseFields=state%2Ctype%2Ccase_id%2Ccase_number%2Cauthority_reference_number%2Cmarker%2Cwebgtno%2Cstart_date%2Cend_date%2Capplicant_folder_number%2Cconnected_case%2Cstreet_name%2Capplicant%2Crovm_equipment_type%2Cinitials"
            f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
            f"&pmCaseShowAttachments=false"
            f"&endDateFrom={yesterday}&endDateTo={today}"
            f"&token={vejman_token}"
        ),
        (
            "Nye tilladelser",
            f"https://vejman.vd.dk/permissions/getcases"
            f"?pmCaseStates=3%2C6%2C8%2C12"
            f"&pmCaseFields=state%2Ctype%2Ccase_id%2Ccase_number%2Cauthority_reference_number%2Cmarker%2Cwebgtno%2Cstart_date%2Cend_date%2Capplicant_folder_number%2Cconnected_case%2Cstreet_name%2Capplicant%2Crovm_equipment_type%2Cinitials"
            f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
            f"&pmCaseShowAttachments=false"
            f"&startDateFrom={today}&startDateTo={today}"
            f"&token={vejman_token}"
        ),
    ]

    base_cases_by_number = {}

    for header, url in urls:
        orchestrator_connection.log_info(f"Fetching Vejman list: {header}")
        resp = session.get(url, timeout=60)
        resp.raise_for_status()

        for case in resp.json().get("cases", []):
            if case.get("initials") not in ALLOWED_INITIALS:
                continue
            if not case.get("case_number"):
                continue
            base_cases_by_number[case["case_number"]] = case

    result = []

    for case in base_cases_by_number.values():
        case_id = case.get("case_id")
        if not case_id:
            continue

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

        start_raw = details.get("start_date") or case.get("start_date")
        end_raw = details.get("end_date") or case.get("end_date")

        result.append({
            "case_number": (case.get("case_number") or "").strip(),
            "case_id": str(case_id).strip(),
            "vejman_state": (case.get("state") or "").strip() or None,
            "connected_case": (case.get("connected_case") or "").strip() or None,
            "start_date": datetime.strptime(start_raw, "%d-%m-%Y %H:%M:%S").isoformat() if start_raw else None,
            "end_date": datetime.strptime(end_raw, "%d-%m-%Y %H:%M:%S").isoformat() if end_raw else None,
            "applicant": (case.get("applicant") or "").strip() or None,
            "marker": (case.get("marker") or "").strip() or None,
            "rovm_equipment_type": (case.get("rovm_equipment_type") or "").strip() or None,
            "applicant_folder_number": (case.get("applicant_folder_number") or "").strip() or None,
            "authority_reference_number": (case.get("authority_reference_number") or "").strip() or None,
            "street_status": (site.get("street_status") or "").strip() or None,
            "full_address": full_address,
            "initials": (case.get("initials") or "").strip() or None,
            "source_lat": source_lat,
            "source_lon": source_lon,
        })

    return result


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