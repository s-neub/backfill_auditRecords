#!/usr/bin/env python3
"""
Backfill StoredModel AuditRecords in a ModelOp Center 3.4 Environment
using modelMLC processInstance end times as historical production dates.

This script implements the process described in:
https://modelop.atlassian.net/wiki/spaces/ME/pages/3176890372/Backfilling+StoredModel+AuditRecords+Before+ModelOp+Center+3.4+Upgrade

Key points:
-----------
* We assume you are ALREADY on ModelOp Center 3.4.
* AuditRecords were not previously enabled, so some production models
  are missing expected AuditRecord entries.
* Historical promotion timestamps still exist in MLC execution logs.

Script goals:
-------------
1) **Step 2 (from doc) — Identify existing StoredModels in production**
   - Use the dedicated endpoint:
       GET /api/storedModels/search/findProductionUseCases
     to list StoredModels (UseCases) that are in production.
   - Capture for each:
       - StoredModel ID (UUID)
       - Group (Business Unit)
       - Name, modelStage, createdDate, lastModifiedDate
   - Write: production_storedmodels_from_search.csv
   - Update: StoredModel stage and primary business driver with "unassigned"

2) **Resolve desired production promotion date/time**
   - For each StoredModel from Step 2:
       GET /api/modelMLCs/search/findAllByStoredModelIdAndGroupIn
     and use processInstance.endTime from the last relevant process
     instance as the historical production promotion date.
   - If no such MLC is found, fall back to StoredModel.createdDate.
   - Write: mlc_resolved_production_dates.csv

3) **Create and patch AuditRecords**
   - For each entry:
       POST /model-manage/api/auditRecords
       PATCH /model-manage/api/auditRecords/{id}
     to set createdDate to the historical production date discovered
     from MLCs.
   - Write: auditrecord_backfill_results.csv

WARNING:
--------
This script performs WRITE operations (POST + PATCH) in your 3.4
environment. Test in a non-production clone first.
"""

import json
import logging
import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv


# ==========================================
# 1. CONFIGURATION & AUTHENTICATION
# ==========================================

load_dotenv(override=False)  # Load .env file; don't override existing environment variables

# TODO: Add base url and access token
# Retrieve configuration from environment or prompt user
MOC_BASE_URL = "your-base-url".strip() 
MOC_ACCESS_TOKEN = "your-access-token".strip()

# TODO: Add production stage value
# Production model stage value, from SCCS configuration (Step 1 in doc):
#   modelop:
#     model-stages:
#       production-stage: prod
#
# NOTE: value is case-sensitive and MUST match your environment.
PRODUCTION_MODEL_STAGE_VALUE = "Production" 

#TODO: add production promotion process definition
# Optional filter: which processDefinitionKeys represent promotion-to-prod workflows.
#
# If left empty, the script will:
#   - consider ALL MLCs with non-null processInstance.endTime
#   - pick the one with the LATEST endTime as the production promotion date.
#
# If you know your promotion workflow keys (e.g. "promote-to-prod"), specify them here.
PRODUCTION_PROMOTION_PROCESS_DEFINITION_KEYS: List[str] = [
    # EXAMPLES (commented out):
    # "promote-to-prod",
    # "usecase-prod-pipeline",
    "Update Implementation Stage"
]

# --------------------------------
# StoredModel discovery configuration
# --------------------------------

# We now explicitly use the Step 2 endpoint:
#   GET /api/storedModels/search/findProductionUseCases
#
# This endpoint returns StoredModels / UseCases that are in production.
# We use this as the authoritative discovery mechanism.
PRODUCTION_USECASE_SEARCH_PATH = "/model-manage/api/storedModels/search/findProductionUseCases"

# If, for any reason, the above search endpoint is not available (404),
# we can optionally FALL BACK to listing all StoredModels via:
#   GET /model-manage/api/storedModels
# and then filter locally by modelMetaData.modelStage == PRODUCTION_MODEL_STAGE_VALUE.
ENABLE_FALLBACK_STOREDMODEL_LISTING = True  # set False to fail hard if search endpoint is missing

# We must also get all use cases so we can fill in primary-driver and modelStage information:
# GET /api/storedModels
ALL_STOREDMODEL_PATH = "/model-manage/api/storedModels"

# --------------------------------
# MLC search endpoint configuration
# --------------------------------

# Endpoint you provided for finding MLC executions:
#   GET /api/modelMLCs/search/findAllByStoredModelIdAndGroupIn
MODEL_MLC_SEARCH_PATH = "/model-manage/api/modelMLCs/search/findAllByStoredModelIdAndGroupIn"

# Query parameter names for the MLC search endpoint.
# These MAY vary per environment; confirm against your actual API docs.
#
# Default assumption:
#   GET /api/modelMLCs/search/findAllByStoredModelIdAndGroupIn
#       ?storedModelId=<UUID>&groups=<GROUP>&page=0&size=200
MODEL_MLC_QUERY_PARAM_STORED_MODEL_ID = "storedModelId"  
MODEL_MLC_QUERY_PARAM_GROUPS = "group"                  

# --------------------------------
# CSV OUTPUT LOCATIONS
# --------------------------------

# Step 2 / Initial discovery snapshot:
PRODUCTION_STOREDMODELS_CSV = "production_storedmodels_from_search.csv"

# All stored models
ALL_STOREDMODELS_CSV = "all_storedmodels.csv"

# MLC-derived production dates:
MLC_PRODUCTION_DATES_CSV = "mlc_resolved_production_dates.csv"

# Final POST/PATCH backfill results:
AUDIT_BACKFILL_RESULTS_CSV = "auditrecord_backfill_results.csv"


# --------------------------------
# HTTP / REQUESTS CONFIG
# --------------------------------

VERIFY_SSL = True      # TODO: set False ONLY if you must bypass TLS verification (not recommended)
HTTP_TIMEOUT = 30      # seconds per request
PAGE_SIZE = 200        # page size for list/search endpoints


# --------------------------------
# LOGGING CONFIGURATION
# --------------------------------

logging.basicConfig(
    level=logging.INFO,   # Use logging.DEBUG for verbose output
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("moc_3_4_audit_backfill")


# ==========================================
# 1.5 AUTHENTICATION & ENV FILE MANAGEMENT
# ==========================================

def authenticate_and_get_token(base_url: str, username: str, password: str) -> str:
    """
    Authenticate to ModelOp Center using username and password,
    and retrieve an OAuth2 access token from the /gocli/token endpoint.

    Parameters
    ----------
    base_url : str
        Base URL of ModelOp Center (no trailing slash).
    username : str
        Username for authentication.
    password : str
        Password for authentication.

    Returns
    -------
    str
        OAuth2 access token.

    Raises
    ------
    requests.RequestException
        If authentication fails.
    ValueError
        If token extraction fails.
    """
    token_url = f"{base_url}/gocli/token"
    payload = {"username": username, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    logger.info("Authenticating to ModelOp Center at %s...", base_url)
    try:
        response = requests.post(token_url, data=payload, headers=headers, timeout=HTTP_TIMEOUT, verify=VERIFY_SSL)
        response.raise_for_status()
        
        access_token = response.text.strip()
        logger.debug("Received token from authentication endpoint.")
        
        # Handle JSON response if applicable (some endpoints return raw string, others JSON)
        if "{" in access_token:
            try:
                token_data = json.loads(access_token)
                access_token = token_data.get("access_token", access_token)
            except json.JSONDecodeError:
                pass  # Use raw response if not valid JSON
        
        if not access_token:
            raise ValueError("Could not extract access token from authentication response.")
        
        logger.info("Successfully authenticated to ModelOp Center.")
        return access_token
    except requests.RequestException as exc:
        logger.error("Authentication failed: %s", exc)
        raise


def save_env_file(base_url: str, username: str, password: str, access_token: str, env_path: str = ".env") -> None:
    """
    Save configuration and access token to a .env file for future use.

    Parameters
    ----------
    base_url : str
        ModelOp Center base URL.
    username : str
        Username used for authentication.
    password : str
        Password used for authentication.
    access_token : str
        OAuth2 access token.
    env_path : str, optional
        Path to .env file, by default ".env".
    """
    try:
        with open(env_path, "w") as env_file:
            env_file.write(f"MOC_BASE_URL={base_url}\n")
            env_file.write(f"USERNAME={username}\n")
            env_file.write(f"PASSWORD={password}\n")
            env_file.write(f"MOC_ACCESS_TOKEN={access_token}\n")
        logger.info("Configuration saved to %s", env_path)
    except IOError as exc:
        logger.error("Failed to write .env file: %s", exc)
        raise


# ==========================================
# 2. SHARED HELPERS
# ==========================================

def normalize_access_token(raw_token: str) -> str:
    """
    Normalize an access token that may be provided either as:
      - a plain bearer token string, or
      - a JSON string containing {"access_token": "<token>"}.

    This mirrors environments where tooling may return the full OAuth2
    token payload JSON instead of just the access_token string.

    Parameters
    ----------
    raw_token : str
        Raw token string, possibly containing JSON.

    Returns
    -------
    str
        The actual bearer token value.

    Raises
    ------
    ValueError
        If the token is empty or JSON cannot be parsed as expected.
    """
    raw_token = (raw_token or "").strip()
    if not raw_token:
        raise ValueError("Access token is empty. Please configure MOC_ACCESS_TOKEN.")

    if raw_token.startswith("{") and "access_token" in raw_token:
        try:
            parsed = json.loads(raw_token)
            token = parsed.get("access_token")
            if not token:
                raise ValueError("JSON token string does not contain 'access_token' key.")
            return token
        except json.JSONDecodeError as exc:
            raise ValueError(f"Failed to parse MOC_ACCESS_TOKEN as JSON: {exc}") from exc

    return raw_token


def create_authenticated_session(base_url: str, access_token: str) -> requests.Session:
    """
    Create a `requests.Session` pre-configured with Authorization headers
    for the given ModelOp Center instance.

    Parameters
    ----------
    base_url : str
        Base URL for the MOC instance (no trailing slash).
    access_token : str
        OAuth2 token (plain string or JSON containing 'access_token').

    Returns
    -------
    requests.Session
        Configured HTTP session.
    """
    token = normalize_access_token(access_token)
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    )
    session.verify = VERIFY_SSL
    logger.info("Authenticated HTTP session created for base URL: %s", base_url)
    return session


# ==========================================
# 3. STEP 2 – IDENTIFY EXISTING STOREDMODELS IN PRODUCTION
#    VIA /api/storedModels/search/findProductionUseCases
# ==========================================

def list_production_storedmodels_via_search(
    base_url: str,
    session: requests.Session,
    page_size: int = PAGE_SIZE,
) -> List[Dict]:
    """
    Use the dedicated Step 2 endpoint to identify StoredModels (UseCases)
    already in production:

        GET /api/storedModels/search/findProductionUseCases

    This function pages through the results and returns the full list
    of StoredModel JSON objects.

    Parameters
    ----------
    base_url : str
        ModelOp Center base URL.
    session : requests.Session
        Authenticated session.
    page_size : int, optional
        Page size for requests, by default PAGE_SIZE.

    Returns
    -------
    List[Dict]
        List of StoredModel JSON objects.
    """
    logger.info(
        "Step 2 — Calling /api/storedModels/search/findProductionUseCases to discover production StoredModels..."
    )
    stored_models: List[Dict] = []
    page = 0

    while True:
        url = f"{base_url}{PRODUCTION_USECASE_SEARCH_PATH}"
        params = {"page": page, "size": page_size}
        logger.debug("Requesting production use cases page=%s size=%s", page, page_size)

        resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code == 404:
            logger.error(
                "Endpoint %s not found (404). Verify that your environment exposes "
                "/api/storedModels/search/findProductionUseCases. "
                "You may enable fallback listing (ENABLE_FALLBACK_STOREDMODEL_LISTING) "
                "if this endpoint is unavailable.",
                PRODUCTION_USECASE_SEARCH_PATH,
            )
            raise FileNotFoundError("findProductionUseCases endpoint not available.")

        resp.raise_for_status()
        body = resp.json()
        batch = body.get("_embedded", {}).get("storedModels", [])

        if not batch:
            break

        stored_models.extend(batch)

        page_info = body.get("page", {})
        total_pages = page_info.get("totalPages")
        if total_pages is not None and page >= total_pages - 1:
            break

        page += 1

    logger.info("Discovered %d production StoredModels via findProductionUseCases.", len(stored_models))
    return stored_models


def list_all_stored_models_via_model_manage(
    base_url: str,
    session: requests.Session,
    page_size: int = PAGE_SIZE,
) -> List[Dict]:
    """
    List all Use Case StoredModels from:

        GET /model-manage/api/storedModels/search/inventory?isUseCase=true

    Parameters
    ----------
    base_url : str
        ModelOp Center base URL.
    session : requests.Session
        Authenticated session.
    page_size : int, optional
        Page size for each request.

    Returns
    -------
    List[Dict]
        List of StoredModel JSON objects.
    """
    logger.info("Fallback — Listing all StoredModels via /model-manage/api/storedModels/search/inventory...")
    stored_models: List[Dict] = []
    page = 0

    while True:
        url = f"{base_url}/model-manage/api/storedModels/search/inventory"
        params = {"isUseCase": "true", "page": page, "size": page_size}
        resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()

        batch = body.get("_embedded", {}).get("storedModels", [])
        if not batch:
            break

        stored_models.extend(batch)

        page_info = body.get("page", {})
        total_pages = page_info.get("totalPages")
        if total_pages is not None and page >= total_pages - 1:
            break

        page += 1

    logger.info("Retrieved %d StoredModels.", len(stored_models))
    return stored_models


def filter_production_stored_models(
    stored_models: List[Dict],
    production_stage: str,
) -> List[Dict]:
    """
    Filter StoredModels based on modelMetaData.modelStage == production_stage.

    Used in fallback scenario when we list all StoredModels via
    /model-manage/api/storedModels.

    Parameters
    ----------
    stored_models : List[Dict]
        List of StoredModel JSON objects.
    production_stage : str
        Production stage value, e.g., "prod".

    Returns
    -------
    List[Dict]
        Filtered list of StoredModels currently in production.
    """
    result: List[Dict] = []
    for sm in stored_models:
        meta = sm.get("modelMetaData", {}) or {}
        stage = meta.get("modelStage")
        if stage == production_stage:
            result.append(sm)
    return result


def discover_production_storedmodels(
    base_url: str,
    session: requests.Session,
    production_stage: str,
    csv_path: str,
) -> List[Dict]:
    """
    Step 2 implementation (plus persistence):

    1. If that endpoint is not available AND ENABLE_FALLBACK_STOREDMODEL_LISTING is True:
         - Call GET /model-manage/api/storedModels
         - Filter by modelMetaData.modelStage == production_stage

    3. For each discovered StoredModel, capture:
         - storedModelId (UUID)
         - group (Business Unit)
         - name
         - modelStage
         - createdDate
         - lastModifiedDate

       Write these to `csv_path` so you have a durable snapshot of
       "existing StoredModels in production" BEFORE backfilling AuditRecords.

    Returns the in-memory list of StoredModel dicts used for subsequent
    MLC-based production date resolution.

    Parameters
    ----------
    base_url : str
        ModelOp Center base URL.
    session : requests.Session
        Authenticated session.
    production_stage : str
        Production model stage value (e.g., "prod").
    csv_path : str
        CSV path for the discovery snapshot.

    Returns
    -------
    List[Dict]
        List of StoredModel JSON objects discovered by this step.
    """

    try:
        stored_models = list_production_storedmodels_via_search(base_url, session)
        all_models = list_all_stored_models_via_model_manage(base_url, session)

    except FileNotFoundError:
        if not ENABLE_FALLBACK_STOREDMODEL_LISTING:
            logger.error(
                "Production use-case search endpoint is not available and fallback listing is disabled. "
                "Cannot proceed with discovery."
            )
            return []
        logger.warning(
            "Production use-case search endpoint unavailable. "
            "Falling back to /model-manage/api/storedModels + local production-stage filter."
        )
        all_models = list_all_stored_models_via_model_manage(base_url, session)
        stored_models = filter_production_stored_models(all_models, production_stage)

    if not stored_models:
        logger.warning("No production StoredModels discovered in Step 2.")
        return []

    # add primary driver and model stage values
    for model in all_models:
        m = model.get("modelMetaData")
        if m.get("modelStage") == "":
            m["modelStage"] = "unassigned"
        if m.get("useCaseInfo"):
            if m.get("useCaseInfo").get("business"):
                if m.get("useCaseInfo").get("business").get("primaryDriver") == "":
                    m["useCaseInfo"]["business"]["primaryDriver"] = "unassigned"
        request_body = {"modelMetaData": {"modelStage": m.get("modelStage", "unassigned"), "useCaseInfo": {"business": {"primaryDriver": "unassigned"}}}}
        a = session.patch(f"{base_url}/model-manage/api/storedModels/{model.get("id")}",data=json.dumps(request_body))

    # Build a tabular snapshot.
    rows: List[Dict] = []
    for sm in stored_models:
        meta = sm.get("modelMetaData", {}) or {}
        rows.append(
            {
                "storedModelId": sm.get("id"),
                "storedModelName": meta.get("name", sm.get("id")),
                "group": sm.get("group", "UNKNOWN_GROUP"),
                "modelStage": meta.get("modelStage"),
                "createdDate": sm.get("createdDate"),
                "lastModifiedDate": sm.get("lastModifiedDate"),
            }
        )

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    logger.info(
        "Step 2 snapshot written: %d production StoredModels -> %s",
        len(df),
        csv_path,
    )
    return stored_models, all_models


# ==========================================
# 4. RESOLVE PRODUCTION DATES FROM MLC LOGS
# ==========================================

def fetch_model_mlcs_for_stored_model(
    base_url: str,
    session: requests.Session,
    stored_model_id: str,
    group: str,
    page_size: int = PAGE_SIZE,
) -> List[Dict]:
    """
    Fetch modelMLCs for a specific StoredModel+group combination.

    Endpoint (per user example):
        GET /api/modelMLCs/search/findAllByStoredModelIdAndGroupIn

    Default query parameter mapping (configurable via constants):
        storedModelId=<UUID>&groups=<GROUP>&page=0&size=200

    Parameters
    ----------
    base_url : str
        ModelOp Center base URL.
    session : requests.Session
        Authenticated session.
    stored_model_id : str
        StoredModel UUID.
    group : str
        StoredModel group (Business Unit).
    page_size : int, optional
        Page size for pagination.

    Returns
    -------
    List[Dict]
        List of modelMLC JSON objects associated with the StoredModel.
    """
    logger.debug(
        "Fetching modelMLCs for StoredModel id=%s group=%s...",
        stored_model_id,
        group,
    )

    mlcs: List[Dict] = []
    page = 0

    while True:
        url = f"{base_url}{MODEL_MLC_SEARCH_PATH}"
        params = {
            MODEL_MLC_QUERY_PARAM_STORED_MODEL_ID: stored_model_id,
            MODEL_MLC_QUERY_PARAM_GROUPS: group,
            "page": page,
            "size": page_size,
        }

        resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code == 404:
            logger.warning(
                "MLC search endpoint returned 404 for StoredModel %s / group %s. No MLC records found.",
                stored_model_id,
                group,
            )
            break

        resp.raise_for_status()
        body = resp.json()
        batch = body.get("_embedded", {}).get("modelMLCs", [])

        if not batch:
            break

        mlcs.extend(batch)

        page_info = body.get("page", {})
        total_pages = page_info.get("totalPages")
        if total_pages is not None and page >= total_pages - 1:
            break

        page += 1

    logger.debug(
        "Found %d modelMLC entries for StoredModel id=%s group=%s.",
        len(mlcs),
        stored_model_id,
        group,
    )
    return mlcs


def extract_latest_mlc_process_end_time(
    model_mlcs: List[Dict],
    allowed_process_definition_keys: Optional[List[str]] = None,
) -> Optional[Tuple[str, Dict, Dict]]:
    """
    From a list of modelMLC objects, extract the processInstance.endTime
    for the LAST relevant processInstance.

    Rationale:
    ----------
    * Historical promotions to production are encoded as BPMN processes.
    * The final "promotion" instance we care about is the latest one whose
      processInstance.endTime is non-null.
    * If specific promotion processDefinitionKeys are known, we can filter
      strictly to those. Otherwise we consider all MLCs with endTime.

    Behavior:
    ---------
      - If allowed_process_definition_keys is non-empty:
            consider only MLCs whose processInstance.processDefinitionKey
            is in that whitelist.
      - Otherwise:
            consider all MLCs with non-null endTime.
      - Among candidates, pick the entry with MAX endTime
        (ISO 8601 strings sort lexicographically).

    Parameters
    ----------
    model_mlcs : List[Dict]
        List of modelMLC JSON objects.
    allowed_process_definition_keys : List[str], optional
        Optional whitelist of processDefinitionKey values representing
        promotion-to-prod processes.

    Returns
    -------
    Optional[Tuple[str, Dict, Dict]]
        (endTime, mlc_object, processInstance_object), or None if no
        suitable candidate is found.
    """
    if not model_mlcs:
        return None

    candidates: List[Tuple[str, Dict, Dict]] = []

    for mlc in model_mlcs:
        proc = mlc.get("processInstance") or {}
        end_time = proc.get("endTime")
        proc_def_key = proc.get("processDefinitionKey")

        if allowed_process_definition_keys:
            if proc_def_key not in allowed_process_definition_keys:
                continue

        if not end_time:
            continue

        candidates.append((end_time, mlc, proc))

    if not candidates:
        return None

    candidates.sort(key=lambda t: t[0])
    latest_end_time, latest_mlc, latest_proc = candidates[-1]
    return latest_end_time, latest_mlc, latest_proc


def resolve_production_dates_from_mlcs(
    base_url: str,
    session: requests.Session,
    targets: List[Dict],
    csv_path: str,
) -> pd.DataFrame:
    """
    For each target StoredModel, derive the "desired production promotion
    date/time" using MLC logs:

    - GET /api/modelMLCs/search/findAllByStoredModelIdAndGroupIn
    - Use processInstance.endTime from the last relevant processInstance
      as the production date.
    - If no such MLC is found, fall back to StoredModel.createdDate.

    This answers the "where should the historical createdDate come from?"
    question that the Confluence page does not specify: we derive it from
    existing MLC workflow logs instead of guessing.

    CSV columns written:
        storedModelId
        storedModelName
        group
        resolvedProductionDate
        resolvedProductionSource
        mlcId
        processInstanceId
        processDefinitionKey
        processDefinitionName
        processStartTime
        processEndTime
        storedModelCreatedDate
        storedModelLastModifiedDate

    Parameters
    ----------
    base_url : str
        ModelOp Center base URL.
    session : requests.Session
        Authenticated session.
    targets : List[Dict]
        StoredModels obtained from Step 2 discovery.
    csv_path : str
        Path to write MLC-based production date resolutions.

    Returns
    -------
    pd.DataFrame
        DataFrame of resolved production dates.
    """
    logger.info("=== Step 3 — Resolving 'desired production promotion date/time' from modelMLC workflows ===")

    rows: List[Dict] = []

    for sm in targets:
        sm_id = sm.get("id")
        if not sm_id:
            logger.warning("StoredModel missing id field. Skipping.")
            continue
        sm_id = str(sm_id)
        sm_group = sm.get("group", "UNKNOWN_GROUP")
        meta = sm.get("modelMetaData", {}) or {}
        sm_name = meta.get("name", sm_id)
        sm_created = sm.get("createdDate")
        sm_last_modified = sm.get("lastModifiedDate")

        logger.info(
            "Resolving production date for StoredModel id=%s name=%s group=%s...",
            sm_id,
            sm_name,
            sm_group,
        )

        mlcs = fetch_model_mlcs_for_stored_model(
            base_url=base_url,
            session=session,
            stored_model_id=sm_id,
            group=sm_group,
        )

        latest_info = extract_latest_mlc_process_end_time(
            mlcs,
            allowed_process_definition_keys=(
                PRODUCTION_PROMOTION_PROCESS_DEFINITION_KEYS or None
            ),
        )

        if latest_info:
            end_time, mlc_obj, proc = latest_info
            logger.info(
                "Production date resolved from MLC endTime=%s (processDefinitionKey=%s).",
                end_time,
                proc.get("processDefinitionKey"),
            )
            resolved_date = end_time
            resolved_source = "MODEL_MLC_PROCESS_INSTANCE"
            mlc_id = mlc_obj.get("id")
            proc_id = proc.get("id")
            proc_def_key = proc.get("processDefinitionKey")
            proc_def_name = proc.get("processDefinitionName")
            proc_start = proc.get("startTime")
            proc_end = proc.get("endTime")
        else:
            logger.warning(
                "No MLC endTime found for StoredModel %s. Falling back to StoredModel.createdDate=%s.",
                sm_id,
                sm_created,
            )
            resolved_date = sm_created
            resolved_source = "STORED_MODEL_CREATED_DATE"
            mlc_id = None
            proc_id = None
            proc_def_key = None
            proc_def_name = None
            proc_start = None
            proc_end = None

        rows.append(
            {
                "storedModelId": sm_id,
                "storedModelName": sm_name,
                "group": sm_group,
                "resolvedProductionDate": resolved_date,
                "resolvedProductionSource": resolved_source,
                "mlcId": mlc_id,
                "processInstanceId": proc_id,
                "processDefinitionKey": proc_def_key,
                "processDefinitionName": proc_def_name,
                "processStartTime": proc_start,
                "processEndTime": proc_end,
                "storedModelCreatedDate": sm_created,
                "storedModelLastModifiedDate": sm_last_modified,
            }
        )

    if not rows:
        logger.warning(
            "No production dates were resolved from MLC logs. "
            "mlc_resolved_production_dates.csv will not be written."
        )
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    logger.info(
        "Resolved production dates for %d StoredModels from MLC logs. CSV: %s",
        len(df),
        csv_path,
    )
    return df


# ==========================================
# 5. CREATE & PATCH AUDIT RECORDS (POST + PATCH)
# ==========================================

def post_audit_record(
    base_url: str,
    session: requests.Session,
    group: str,
    stored_model_id: str,
    production_stage: str,
) -> Dict:
    """
    Create an AuditRecord for a StoredModel in the 3.4 environment.

    Implements Step 4 in the Confluence guide:

        POST /model-manage/api/auditRecords

    We follow the documented structure and set modelStage via 'changes'.

    Parameters
    ----------
    base_url : str
        ModelOp Center base URL.
    session : requests.Session
        Authenticated session.
    group : str
        StoredModel group (Business Unit).
    stored_model_id : str
        StoredModel UUID.
    production_stage : str
        Production model stage value (e.g., "prod").

    Returns
    -------
    Dict
        JSON body of the created AuditRecord.
    """
    url = f"{base_url}/model-manage/api/auditRecords"
    payload = {
        "group": group,
        "metaData": {
            "custom": {},
            "tags": [],
            "modelStage": "unassigned",
        },
        "entityId": stored_model_id,
        "entityType": "StoredModel",
        "changes": [
            {"op": "remove", "path": "/modelMetaData/tags"},
            {"op": "replace", "path": "/modelMetaData/modelStage", "value": production_stage},
        ],
        "storedModelId": stored_model_id,
    }

    logger.debug(
        "POSTing AuditRecord for StoredModel %s (group=%s)...",
        stored_model_id,
        group,
    )
    resp = session.post(url, data=json.dumps(payload), timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    record = resp.json()

    logger.info(
        "Created AuditRecord for StoredModel %s. newAuditRecordId=%s",
        stored_model_id,
        record.get("id"),
    )
    return record


def patch_audit_record_created_date(
    base_url: str,
    session: requests.Session,
    audit_record_id: str,
    historical_created_date: str,
) -> Dict:
    """
    PATCH an AuditRecord's createdDate to the resolved historical
    production promotion date.

    This is Step 6 / Step 7 from the Confluence guide:

        PATCH /model-manage/api/auditRecords/{AUDIT_RECORD_ID}
        {
          "createdDate": "<<MODEL_PRODUCTION_DATE>>"
        }

    Parameters
    ----------
    base_url : str
        ModelOp Center base URL.
    session : requests.Session
        Authenticated session.
    audit_record_id : str
        ID of the AuditRecord to patch.
    historical_created_date : str
        ISO 8601 UTC timestamp representing the historical production
        promotion date (e.g., "2025-12-30T14:23:45.123Z").

    Returns
    -------
    Dict
        JSON body of the updated AuditRecord.
    """
    url = f"{base_url}/model-manage/api/auditRecords/{audit_record_id}"
    payload = {"createdDate": historical_created_date}

    logger.debug(
        "PATCHing AuditRecord %s.createdDate to %s...",
        audit_record_id,
        historical_created_date,
    )
    resp = session.patch(url, data=json.dumps(payload), timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    updated = resp.json()

    logger.info(
        "Patched AuditRecord %s createdDate to %s.",
        audit_record_id,
        updated.get("createdDate"),
    )
    return updated


def backfill_audit_records(
    base_url: str,
    session: requests.Session,
    source_csv_path: str,
    target_csv_path: str,
) -> pd.DataFrame:
    """
    For each StoredModel row in `source_csv_path`, create and patch an
    AuditRecord using the MLC-resolved production date.

    Steps:
      1. Read mlc_resolved_production_dates.csv.
      2. For each row:
           a) POST /model-manage/api/auditRecords
           b) PATCH /model-manage/api/auditRecords/{id} createdDate to
              resolvedProductionDate (if non-null).
      3. Write auditrecord_backfill_results.csv.

    Input CSV columns:
        storedModelId
        storedModelName
        group
        resolvedProductionDate
        resolvedProductionSource
        mlcId
        processInstanceId
        processDefinitionKey
        processDefinitionName
        processStartTime
        processEndTime
        storedModelCreatedDate
        storedModelLastModifiedDate

    Output CSV extends with:
        newAuditRecordId
        newAuditRecordCreatedDate
        newAuditRecordLastModifiedDate

    Parameters
    ----------
    base_url : str
        ModelOp Center base URL.
    session : requests.Session
        Authenticated session.
    source_csv_path : str
        Input CSV file path.
    target_csv_path : str
        Output CSV file path.

    Returns
    -------
    pd.DataFrame
        DataFrame of backfill results.
    """
    logger.info("=== Step 4 — Creating & PATCHing AuditRecords based on MLC production dates ===")

    try:
        df_source = pd.read_csv(source_csv_path)
    except FileNotFoundError as exc:
        logger.error(
            "Source CSV %s not found. Ensure the MLC resolution step completed successfully.",
            source_csv_path,
        )
        raise exc

    if df_source.empty:
        logger.warning("Source CSV %s is empty. No AuditRecords will be backfilled.", source_csv_path)
        return df_source

    rows_out: List[Dict] = []

    for _, row in df_source.iterrows():
        sm_id = str(row["storedModelId"])
        sm_name = str(row.get("storedModelName", "Unknown Name"))
        group = str(row.get("group", "UNKNOWN_GROUP"))
        resolved_prod_date = row.get("resolvedProductionDate", row.get("storedModelLastModifiedDate"))
        resolved_source = row.get("resolvedProductionSource")
        mlc_id = row.get("mlcId")
        proc_id = row.get("processInstanceId")
        proc_def_key = row.get("processDefinitionKey")
        proc_def_name = row.get("processDefinitionName")
        proc_start = row.get("processStartTime")
        proc_end = row.get("processEndTime")
        sm_created = row.get("storedModelCreatedDate")
        sm_last_modified = row.get("storedModelLastModifiedDate")

        logger.info(
            "Backfilling AuditRecord for StoredModel id=%s name=%s group=%s "
            "(resolvedSource=%s, mlcId=%s, processInstanceId=%s)...",
            sm_id,
            sm_name,
            group,
            resolved_source,
            mlc_id,
            proc_id,
        )

        # 2a. POST new AuditRecord
        created_record = post_audit_record(
            base_url=base_url,
            session=session,
            group=group,
            stored_model_id=sm_id,
            production_stage=PRODUCTION_MODEL_STAGE_VALUE,
        )

        new_ar_id = created_record.get("id")
        new_created = created_record.get("createdDate")
        new_last_modified = created_record.get("lastModifiedDate")

        # 2b. PATCH createdDate to resolvedProductionDate
        patched_record = created_record
        if resolved_prod_date and not pd.isna(resolved_prod_date):
            patched_record = patch_audit_record_created_date(
                base_url=base_url,
                session=session,
                audit_record_id=str(new_ar_id),
                historical_created_date=str(resolved_prod_date),
            )
            new_created = patched_record.get("createdDate")
            new_last_modified = patched_record.get("lastModifiedDate")
        else:
            logger.warning(
                "No resolvedProductionDate for StoredModel %s; "
                "AuditRecord created but createdDate NOT patched.",
                sm_id,
            )

        rows_out.append(
            {
                "storedModelId": sm_id,
                "storedModelName": sm_name,
                "group": group,
                "resolvedProductionDate": resolved_prod_date,
                "resolvedProductionSource": resolved_source,
                "mlcId": mlc_id,
                "processInstanceId": proc_id,
                "processDefinitionKey": proc_def_key,
                "processDefinitionName": proc_def_name,
                "processStartTime": proc_start,
                "processEndTime": proc_end,
                "storedModelCreatedDate": sm_created,
                "storedModelLastModifiedDate": sm_last_modified,
                "newAuditRecordId": new_ar_id,
                "newAuditRecordCreatedDate": new_created,
                "newAuditRecordLastModifiedDate": new_last_modified,
            }
        )

    df_out = pd.DataFrame(rows_out)
    df_out.to_csv(target_csv_path, index=False)
    logger.info(
        "AuditRecord backfill complete. %d records processed. Results CSV: %s",
        len(df_out),
        target_csv_path,
    )
    return df_out


# ==========================================
# 6. MAIN ORCHESTRATION
# ==========================================

def main() -> None:
    """
    Orchestrate the full AuditRecord backfill process in a single 3.4 environment.

    Summary of steps:
    -----------------
    1) Authenticate to current 3.4 environment (MOC_BASE_URL + MOC_ACCESS_TOKEN).
       - If token not in .env, authenticate with username/password and save to .env.

    2) **Step 2 — Identify existing StoredModels in production**
       - Call GET /api/storedModels/search/findProductionUseCases
       - Capture:
           * StoredModel ID (UUID)
           * Group (Business Unit)
           * Name, modelStage, createdDate, lastModifiedDate
       - Write: production_storedmodels_from_search.csv

    3) **Resolve desired production promotion date/time from MLC logs**
       - For each StoredModel from step 2:
           * GET /api/modelMLCs/search/findAllByStoredModelIdAndGroupIn
           * Use processInstance.endTime from last relevant process as
             the historical production timestamp.
           * If no MLC is found, fall back to StoredModel.createdDate.
       - Write: mlc_resolved_production_dates.csv

    4) **Create & PATCH AuditRecords**
       - For each row in mlc_resolved_production_dates.csv:
           * POST /model-manage/api/auditRecords
           * PATCH /model-manage/api/auditRecords/{id} createdDate to 
             resolvedProductionDate.
       - Write: auditrecord_backfill_results.csv

    After completion:
    -----------------
    * All previously unmanaged production models will have AuditRecords
      with createdDate set to historical production dates derived from
      the original MLC workflows.
    * 3.4 dashboard components can then render accurate timelines and
      metrics for those models.
    """
    # Step 0: Handle authentication and .env file management
    global MOC_ACCESS_TOKEN  # Allow modification of the global variable
    
    if not MOC_ACCESS_TOKEN:
        logger.info("No access token found in environment. Authenticating with username/password...")
        try:
            MOC_ACCESS_TOKEN = authenticate_and_get_token(MOC_BASE_URL, USERNAME, PASSWORD)
            save_env_file(MOC_BASE_URL, USERNAME, PASSWORD, MOC_ACCESS_TOKEN)
            logger.info("Token obtained and saved to .env file for future use.")
        except Exception as exc:
            logger.error("Failed to authenticate: %s", exc)
            return
    else:
        logger.info("Access token loaded from environment. Proceeding with existing token.")
    
    # Step 1: Authenticate to MOC
    logger.info("Authenticating to ModelOp Center 3.4 environment: %s", MOC_BASE_URL)
    session = create_authenticated_session(MOC_BASE_URL, MOC_ACCESS_TOKEN)

    # Step 2: Discover existing StoredModels in production
    targets = discover_production_storedmodels(
        base_url=MOC_BASE_URL,
        session=session,
        production_stage=PRODUCTION_MODEL_STAGE_VALUE,
        csv_path=PRODUCTION_STOREDMODELS_CSV,
    )
    #print("line 1278" + str(len(targets)))
    #print(targets[0][0])
    if not targets:
        logger.error(
            "No production StoredModels discovered via Step 2. "
            "Aborting before resolving production dates or creating AuditRecords."
        )
        return

    # Step 3: Resolve production dates from MLC logs
    df_mlc = resolve_production_dates_from_mlcs(
        base_url=MOC_BASE_URL,
        session=session,
        targets=targets[0],
        csv_path=MLC_PRODUCTION_DATES_CSV,
    )
    if df_mlc.empty:
        logger.error(
            "No production dates were resolved from MLC logs. "
            "Aborting before creating any AuditRecords."
        )
        return

    # Step 4: Backfill AuditRecords (POST + PATCH)
    backfill_audit_records(
        base_url=MOC_BASE_URL,
        session=session,
        source_csv_path=MLC_PRODUCTION_DATES_CSV,
        target_csv_path=AUDIT_BACKFILL_RESULTS_CSV,
    )

    logger.info("AuditRecord backfill script completed successfully.")


if __name__ == "__main__":
    main()