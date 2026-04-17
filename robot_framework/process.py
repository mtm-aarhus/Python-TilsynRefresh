from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

from functools import lru_cache
from math import radians, cos, sin, asin, sqrt
import hashlib
import json
import re
import urllib.parse

import pyodbc
import requests
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from office365.sharepoint.client_context import ClientContext

from pez import fetch_pez_cases
from vejman import fetch_vejman_cases



CVR_API_URL = "https://cvrapi.dk/api"
USER_AGENT = "AAK Tilsyn"
DEPOT = (56.161147, 10.13455)
SHAREPOINT_TOP_FOLDER = "Delte dokumenter/Tilladelser"
SHAREPOINT_LINK_BASE = "https://aarhuskommune.sharepoint.com/Teams/tea-teamsite10014/Delte%20dokumenter/Tilladelser"

DEPOT_NEAR_ALLOWED_ADDRESS_PARTS = [
    "karen blixens",
    "edwin rahrs vej",
    "inger christensens gade",
    "lottesvej",
    "hejredalsvej",
]


def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    orchestrator_connection.log_trace("Running unified TilsynItems sync.")

    vejman_token = orchestrator_connection.get_credential("VejmanToken").password

    pez_cred = orchestrator_connection.get_credential("PEZUI")
    pez_username = pez_cred.username
    pez_password = pez_cred.password

    cosmos_credentials = orchestrator_connection.get_credential("AAKTilsynDB")
    cosmos_url = cosmos_credentials.username
    cosmos_key = cosmos_credentials.password

    client = CosmosClient(cosmos_url, credential=cosmos_key)
    container = client.get_database_client("aak-tilsyn").get_container_client("TilsynItems")

    session = requests.Session()
    session.headers.update({
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,en-AU;q=0.8,en-CA;q=0.7,en-IN;q=0.6,en-IE;q=0.5,en-NZ;q=0.4,en-GB-oxendict;q=0.3,en-GB;q=0.2,en-ZA;q=0.1",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0",
    })

    # --- SQL Server (for VejmanTilladelser SharePoint tracking) ---
    sql_server = orchestrator_connection.get_constant("SqlServer")
    sql_conn = pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={sql_server.value};DATABASE=PYORCHESTRATOR;Trusted_Connection=yes;"
    )
    sql_cursor = sql_conn.cursor()

    # --- SharePoint client (for folder creation) ---
    sharepoint_site_base = orchestrator_connection.get_constant("AarhusKommuneSharePoint").value
    sharepoint_site = f"{sharepoint_site_base}/teams/tea-teamsite10014"
    sp_cert = orchestrator_connection.get_credential("SharePointCert")
    sp_api = orchestrator_connection.get_credential("SharePointAPI")
    sp_ctx = ClientContext(sharepoint_site).with_client_certificate(
        tenant=sp_api.username,
        client_id=sp_api.password,
        thumbprint=sp_cert.username,
        cert_path=sp_cert.password,
    )
    sp_ctx.load(sp_ctx.web)
    sp_ctx.execute_query()
    orchestrator_connection.log_trace(f"SharePoint connected: {sp_ctx.web.properties['Title']}")

    created = 0
    updated = 0
    unchanged = 0

    # -------------------------
    # PEZ / henstillinger
    # -------------------------
    pez_cases = fetch_pez_cases(
        orchestrator_connection=orchestrator_connection,
        session=session,
        username=pez_username,
        password=pez_password,
    )

    orchestrator_connection.log_info(f"PEZ candidate cases: {len(pez_cases)}")

    for case in pez_cases:
        company_name = case["company_name"] or get_company_name(case["cvr"]) or "Ugyldigt CVR"
        case_had_existing_rows = False
        case_created_new_rows = False

        for forseelse in case["forseelser"]:
            doc_id = f"{case['henstilling_id']}_{forseelse['nummer']}"

            try:
                existing = container.read_item(item=doc_id, partition_key=doc_id)
                case_had_existing_rows = True
            except CosmosResourceNotFoundError:
                existing = None

            if existing and existing.get("FakturaStatus") != "Ny":
                continue

            street_name = normalize_street_name(case["full_address"])
            location_hash = make_hash({
                "street_name": street_name,
                "source_lat": case["source_lat"],
                "source_lon": case["source_lon"],
            })

            if existing and existing.get("location_hash") == location_hash:
                latitude = existing.get("latitude")
                longitude = existing.get("longitude")
            else:
                latitude, longitude = resolve_coordinates(
                    full_address=case["full_address"],
                    source_lat=case["source_lat"],
                    source_lon=case["source_lon"],
                    fallback_lat=existing.get("latitude") if existing else None,
                    fallback_lon=existing.get("longitude") if existing else None,
                )

            content_hash = make_hash({
                "type": "henstilling",
                "HenstillingId": case["henstilling_id"],
                "PEZUUID": case["case_uuid"],
                "ForseelseNr": forseelse["nummer"],
                "Forseelse": forseelse["text"],
                "CVR": case["cvr"],
                "FirmaNavn": company_name,
                "full_address": case["full_address"],
                "start_date": case["start_date"],
                "location_hash": location_hash,
            })

            if existing:
                if existing.get("content_hash") == content_hash:
                    unchanged += 1
                    continue

                # Only patch sync-owned fields — never touch app fields
                sync_fields = {
                    "HenstillingId": case["henstilling_id"],
                    "PEZUUID": case["case_uuid"],
                    "ForseelseNr": forseelse["nummer"],
                    "Forseelse": forseelse["text"],
                    "CVR": case["cvr"],
                    "FirmaNavn": company_name,
                    "street_name": street_name,
                    "full_address": case["full_address"],
                    "latitude": latitude,
                    "longitude": longitude,
                    "start_date": case["start_date"],
                    "location_hash": location_hash,
                    "content_hash": content_hash,
                }

                # Only set Tilladelsestype if it hasn't been set yet
                if existing.get("Tilladelsestype") is None:
                    sync_fields["Tilladelsestype"] = forseelse["tilladelsestype"]

                patch_in_batches(container, doc_id, sync_fields)
                updated += 1

            else:
                # New doc — set everything including app-field defaults
                doc = {
                    "id": doc_id,
                    "type": "henstilling",
                    "HenstillingId": case["henstilling_id"],
                    "PEZUUID": case["case_uuid"],
                    "ForseelseNr": forseelse["nummer"],
                    "Forseelse": forseelse["text"],
                    "CVR": case["cvr"],
                    "FirmaNavn": company_name,
                    "street_name": street_name,
                    "full_address": case["full_address"],
                    "latitude": latitude,
                    "longitude": longitude,
                    "start_date": case["start_date"],
                    "end_date": case["start_date"],
                    "Kvadratmeter": None,
                    "Tilladelsestype": forseelse["tilladelsestype"],
                    "FakturaStatus": "Ny",
                    "location_hash": location_hash,
                    "content_hash": content_hash,
                }
                container.upsert_item(body=doc)
                created += 1
                case_created_new_rows = True


    # -------------------------
    # Vejman / permissions
    # -------------------------
    vejman_cases = fetch_vejman_cases(
        orchestrator_connection=orchestrator_connection,
        session=session,
        vejman_token=vejman_token,
    )

    orchestrator_connection.log_info(f"Vejman candidate cases: {len(vejman_cases)}")

    for case in vejman_cases:
        doc_id = case["case_id"]
        case_number = case["case_number"]

        try:
            existing = container.read_item(item=doc_id, partition_key=doc_id)
        except CosmosResourceNotFoundError:
            existing = None

        street_name = normalize_street_name(case["full_address"])
        location_hash = make_hash({
            "street_name": street_name,
            "source_lat": case["source_lat"],
            "source_lon": case["source_lon"],
        })

        if existing and existing.get("location_hash") == location_hash:
            latitude = existing.get("latitude")
            longitude = existing.get("longitude")
        else:
            latitude, longitude = resolve_coordinates(
                full_address=case["full_address"],
                source_lat=case["source_lat"],
                source_lon=case["source_lon"],
                fallback_lat=existing.get("latitude") if existing else None,
                fallback_lon=existing.get("longitude") if existing else None,
            )

        # --- SharePoint folder + link ---
        # Use street_name_raw (unmodified from Vejman API) to match dispatcher folder naming
        sharepoint_link = ensure_sharepoint_folder(
            orchestrator_connection, sp_ctx, sql_cursor, sql_conn,
            doc_id, case_number, case.get("street_name_raw") or "Intet vejnavn",
        )

        content_hash = make_hash({
            "type": "permission",
            "case_number": case_number,
            "case_id": case["case_id"],
            "vejman_state": case["vejman_state"],
            "connected_case": case["connected_case"],
            "start_date": case["start_date"],
            "end_date": case["end_date"],
            "applicant": case["applicant"],
            "rovm_equipment_type": case["rovm_equipment_type"],
            "full_address": case["full_address"],
            "location_hash": location_hash,
        })

        if existing:
            if existing.get("content_hash") == content_hash and existing.get("sharepoint_link") == sharepoint_link:
                unchanged += 1
                continue

            sync_fields = {
                "case_number": case_number,
                "case_id": case["case_id"],
                "vejman_state": case["vejman_state"],
                "connected_case": case["connected_case"],
                "start_date": case["start_date"],
                "end_date": case["end_date"],
                "applicant": case["applicant"],
                "marker": case["marker"],
                "rovm_equipment_type": case["rovm_equipment_type"],
                "applicant_folder_number": case["applicant_folder_number"],
                "authority_reference_number": case["authority_reference_number"],
                "street_status": case["street_status"],
                "street_name": street_name,
                "full_address": case["full_address"],
                "initials": case["initials"],
                "latitude": latitude,
                "longitude": longitude,
                "sharepoint_link": sharepoint_link,
                "location_hash": location_hash,
                "content_hash": content_hash,
            }

            patch_in_batches(container, doc_id, sync_fields)
            updated += 1

        else:
            doc = {
                "id": doc_id,
                "type": "permission",
                "case_number": case_number,
                "case_id": case["case_id"],
                "vejman_state": case["vejman_state"],
                "connected_case": case["connected_case"],
                "start_date": case["start_date"],
                "end_date": case["end_date"],
                "applicant": case["applicant"],
                "marker": case["marker"],
                "rovm_equipment_type": case["rovm_equipment_type"],
                "applicant_folder_number": case["applicant_folder_number"],
                "authority_reference_number": case["authority_reference_number"],
                "street_status": case["street_status"],
                "street_name": street_name,
                "full_address": case["full_address"],
                "initials": case["initials"],
                "latitude": latitude,
                "longitude": longitude,
                "sharepoint_link": sharepoint_link,
                "location_hash": location_hash,
                "content_hash": content_hash,
            }
            container.upsert_item(body=doc)
            created += 1

    sql_cursor.close()
    sql_conn.close()

    orchestrator_connection.log_info(
        f"Unified sync done. Created={created}, Updated={updated}, Unchanged={unchanged}"
    )


def patch_in_batches(container, doc_id: str, fields: dict):
    """Patch only the given fields on an existing Cosmos doc.
    Cosmos limits patch to 10 operations per call, so we batch.
    content_hash is always placed in the last batch — if an earlier
    batch fails, the old hash remains and the next sync retries."""

    # Ensure content_hash goes last
    items = [(k, v) for k, v in fields.items() if k != "content_hash"]
    if "content_hash" in fields:
        items.append(("content_hash", fields["content_hash"]))

    for i in range(0, len(items), 10):
        batch = items[i:i + 10]
        ops = [{"op": "set", "path": f"/{key}", "value": value} for key, value in batch]
        container.patch_item(item=doc_id, partition_key=doc_id, patch_operations=ops)


def normalize_street_name(text: str | None) -> str | None:
    if not text:
        return None
    value = str(text).strip()
    value = value.split(" - ", 1)[0].strip()
    match = re.search(r"\d", value)
    if match:
        value = value[:match.start()].strip()
    return value or None


def clean_address_for_geocoding(text: str | None) -> str | None:
    if not text:
        return None
    value = str(text).strip()
    value = value.split(" - ", 1)[0].strip()
    value = re.sub(r"(\d+[A-Za-z]?)-\d+[A-Za-z]?", r"\1", value)
    return value or None


def resolve_coordinates(full_address: str | None, source_lat, source_lon, fallback_lat=None, fallback_lon=None):
    if source_lat is not None and source_lon is not None:
        if not is_too_close_to_depot(source_lat, source_lon):
            return source_lat, source_lon
        if is_known_valid_depot_area_address(full_address):
            return source_lat, source_lon

    geocode_input = clean_address_for_geocoding(full_address)
    geocoded = geocode_address(geocode_input)
    if geocoded:
        return geocoded

    return fallback_lat, fallback_lon


def geocode_address(address: str | None):
    if not address:
        return None
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{address}, Aarhus, Denmark", "format": "json", "limit": 1},
            headers={"User-Agent": USER_AGENT},
            timeout=5,
        )
        r.raise_for_status()
        data = r.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return None
    return None


def is_known_valid_depot_area_address(text: str | None) -> bool:
    if not text:
        return False
    value = text.casefold()
    return any(part in value for part in DEPOT_NEAR_ALLOWED_ADDRESS_PARTS)


def is_too_close_to_depot(lat, lon, threshold_m=100):
    if lat is None or lon is None:
        return False
    return haversine((lat, lon), DEPOT) <= threshold_m


def haversine(coord1, coord2):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    r = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return r * c


def make_hash(data: dict) -> str:
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ─── SharePoint helpers ──────────────────────────────────────────────────────

def sanitize_folder_name(folder_name: str) -> str:
    """Remove SharePoint-illegal characters. Must match VejmanDispatcher logic."""
    pattern = r'[~#%&*{}\[\]\\:<>?/+|$¤£€\"\t]'
    folder_name = re.sub(pattern, "", folder_name)
    folder_name = re.sub(r"\s+", " ", folder_name).strip()
    return folder_name


def build_sharepoint_link(vejnavn: str, case_number: str) -> str:
    """Build the full SharePoint URL for a permission folder."""
    folder_name = sanitize_folder_name(vejnavn.replace(".", "") + "_" + case_number)
    return f"{SHAREPOINT_LINK_BASE}/{urllib.parse.quote(folder_name)}"


def ensure_sharepoint_folder(
    orchestrator_connection,
    sp_ctx: ClientContext,
    sql_cursor,
    sql_conn,
    case_id: str,
    case_number: str,
    vejnavn: str,
) -> str:
    """Return the SharePoint link for a permission case.
    If the case already exists in SQL, use whatever folder path is stored there.
    If it's new, create the folder + SQL row + queue element.
    Renames are left to the daily VejmanDispatcher.
    """
    sql_cursor.execute(
        "SELECT SharePointFolder FROM [dbo].[VejmanTilladelser] WHERE ID = ?",
        (case_id,),
    )
    row = sql_cursor.fetchone()

    if row:
        # Already tracked — use the existing folder path from SQL as-is
        existing_folder = row[0]
        folder_name = existing_folder.rsplit("/", 1)[-1]
        payload = json.dumps({
        "case_id": case_id,
        "case_number": case_number,
        "sharepoint_folder": f"{SHAREPOINT_TOP_FOLDER}/{folder_name}",
        }, ensure_ascii=False)
        orchestrator_connection.create_queue_element("VejmanPerformer", reference=case_number, data=payload)

        return f"{SHAREPOINT_LINK_BASE}/{urllib.parse.quote(folder_name)}"

    # New case — create folder + SQL row + queue element
    vejnavn_clean = (vejnavn or "Intet vejnavn angivet").replace(".", "")
    folder_name = sanitize_folder_name(vejnavn_clean + "_" + case_number)
    folder_path = f"{SHAREPOINT_TOP_FOLDER}/{folder_name}"

    try:
        top = sp_ctx.web.get_folder_by_server_relative_url(SHAREPOINT_TOP_FOLDER)
        sp_ctx.load(top)
        sp_ctx.execute_query()
        top.folders.add(folder_name).execute_query()
        orchestrator_connection.log_info(f"Created SP folder: {folder_path}")
    except Exception as e:
        orchestrator_connection.log_trace(f"SP folder create (may already exist): {e}")

    sql_cursor.execute(
        "INSERT INTO [dbo].[VejmanTilladelser] (ID, CaseNumber, SharePointFolder, LastUpdated) VALUES (?, ?, ?, GETDATE())",
        (case_id, case_number, folder_path),
    )
    sql_conn.commit()

    payload = json.dumps({
        "case_id": case_id,
        "case_number": case_number,
        "sharepoint_folder": folder_path,
    }, ensure_ascii=False)
    orchestrator_connection.create_queue_element("VejmanPerformer", reference=case_number, data=payload)

    return f"{SHAREPOINT_LINK_BASE}/{urllib.parse.quote(folder_name)}"


@lru_cache(maxsize=5000)
def get_company_name(cvr: str):
    try:
        r = requests.get(
            CVR_API_URL,
            params={"country": "dk", "search": cvr},
            headers={"User-Agent": USER_AGENT},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("name")
    except Exception:
        return None
    return None
