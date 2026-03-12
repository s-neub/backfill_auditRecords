#!/usr/bin/env python3
"""
Preflight Checks for StoredModel AuditRecords Backfill

This script conducts a preflight check before running the actual backfill process:

Step 1: Load Configuration
    - Establish authenticated session to ModelOp Center

Step 2: Discover Production StoredModels
    - GET /api/storedModels/search/findProductionUseCases
    - Capture all StoredModels currently in production

Step 3: Resolve MLC Workflow History
    - For each StoredModel from Step 2:
        GET /api/modelMLCs/search/findAllByStoredModelIdAndGroupIn
    - Capture process execution history

Step 4: Capture Current AuditRecords State
    - For each StoredModel:
        GET /model-manage/api/auditRecords?storedModelId={id}
    - Capture the CURRENT state of audit records BEFORE any backfill operations
    - This allows comparison of before/after results

Output CSVs:
    - preflight_storedmodels.csv        (production StoredModels snapshot)
    - preflight_mlcs.csv                (modelMLC workflow history)
    - preflight_auditrecords_before.csv (current AuditRecords state BEFORE backfill)

These CSVs can be compared against the post-backfill state to verify the operations.
"""

import json
import logging
import os
import re
import time
from typing import Dict, List, Optional

import pandas as pd
import requests

# ==========================================
# CONFIGURATION & AUTHENTICATION
# ==========================================

load_dotenv(override=False)

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

# HTTP Configuration
VERIFY_SSL = True
HTTP_TIMEOUT = 30
PAGE_SIZE = 200

# Output CSV paths
PREFLIGHT_STOREDMODELS_CSV = "preflight_storedmodels.csv"
PREFLIGHT_MLCS_CSV = "preflight_mlcs.csv"
PREFLIGHT_AUDITRECORDS_CSV = "preflight_auditrecords_before.csv"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("preflight_audit_check")


# ==========================================
# AUTHENTICATION & ENV FILE MANAGEMENT
# ==========================================

def normalize_access_token(raw_token: str) -> str:
    """Normalize access token (handle both raw string and JSON formats)."""
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
    """Create authenticated HTTP session."""
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
    logger.info("Authenticated HTTP session created.")
    return session


# ==========================================
# STEP 2: DISCOVER PRODUCTION STOREDMODELS
# ==========================================

def discover_production_storedmodels(base_url: str, session: requests.Session) -> List[Dict]:
    """
    Step 2: Call GET /api/storedModels/search/findProductionUseCases
    to retrieve all StoredModels currently in production.
    """
    logger.info("Step 2 — Discovering production StoredModels...")
    stored_models: List[Dict] = []
    page = 0

    while True:
        url = f"{base_url}/model-manage/api/storedModels/search/findProductionUseCases"
        params = {"page": page, "size": PAGE_SIZE}
        logger.debug("Requesting production use cases page=%s", page)

        resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
        batch = body.get("_embedded", {}).get("storedModels", [])

        if not batch:
            logger.debug("No more StoredModels in this page.")
            break

        stored_models.extend(batch)
        page_info = body.get("page", {})
        total_pages = page_info.get("totalPages")

        if total_pages is not None and page >= total_pages - 1:
            logger.debug("Reached last page of StoredModels.")
            break

        page += 1

    logger.info("Discovered %d production StoredModels.", len(stored_models))
    return stored_models


# ==========================================
# STEP 3: RESOLVE MLC WORKFLOW HISTORY
# ==========================================

def fetch_model_mlcs_for_stored_model(
    base_url: str, session: requests.Session, stored_model_id: str, group: str
) -> List[Dict]:
    """
    Step 3: For each StoredModel, call GET /api/modelMLCs/search/findAllByStoredModelId
    to retrieve workflow execution history.
    """
    logger.debug("Fetching modelMLCs for StoredModel id=%s group=%s...", stored_model_id, group)

    mlcs: List[Dict] = []
    page = 0

    while True:
        url = f"{base_url}/model-manage/api/modelMLCs/search/findAllByStoredModelIdAndGroupIn"
        params = {
            "storedModelId": stored_model_id,
            "group": group,
            "page": page,
            "size": PAGE_SIZE,
        }

        resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
        batch = body.get("_embedded", {}).get("modelMLCs", [])

        if not batch:
            logger.debug("No more MLCs in this page.")
            break

        mlcs.extend(batch)

        page_info = body.get("page", {})
        total_pages = page_info.get("totalPages")
        if total_pages is not None and page >= total_pages - 1:
            logger.debug("Reached last page of MLCs.")
            break

        page += 1

    logger.debug("Found %d modelMLC entries for StoredModel id=%s.", len(mlcs), stored_model_id)
    return mlcs


# ==========================================
# STEP 4: CAPTURE CURRENT AUDITRECORDS STATE
# ==========================================

def fetch_existing_audit_records(base_url: str, session: requests.Session, stored_model_id: str) -> List[Dict]:
    """
    Step 4: For each StoredModel, call GET /model-manage/api/auditRecords
    to capture the CURRENT state of audit records BEFORE any backfill operations.
    """
    logger.debug("Fetching existing AuditRecords for StoredModel id=%s...", stored_model_id)

    audit_records: List[Dict] = []
    page = 0

    while True:
        url = f"{base_url}/model-manage/api/auditRecords/search/findAuditRecordsByStoredModelId"
        params = {
            "storedModelId": stored_model_id,
            "page": page,
            "size": PAGE_SIZE,
        }

        resp = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
        batch = body.get("_embedded", {}).get("auditRecords", [])

        if not batch:
            logger.debug("No AuditRecords found for StoredModel.")
            break

        audit_records.extend(batch)

        page_info = body.get("page", {})
        total_pages = page_info.get("totalPages")
        if total_pages is not None and page >= total_pages - 1:
            logger.debug("Reached last page of AuditRecords.")
            break

        page += 1

    logger.debug("Found %d existing AuditRecords for StoredModel id=%s.", len(audit_records), stored_model_id)
    return audit_records


# ==========================================
# DATA PROCESSING & CSV EXPORT
# ==========================================

def process_and_export_storedmodels(stored_models: List[Dict], csv_path: str) -> pd.DataFrame:
    """Process StoredModels and export to CSV."""
    logger.info("Processing StoredModels data...")
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
                "createdBy": sm.get("createdBy"),
                "lastModifiedBy": sm.get("lastModifiedBy"),
            }
        )

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    logger.info("Exported %d StoredModels to %s", len(df), csv_path)
    return df


def process_and_export_mlcs(
    stored_models: List[Dict], base_url: str, session: requests.Session, csv_path: str
) -> pd.DataFrame:
    """Process MLC data and export to CSV."""
    logger.info("Processing MLC workflow history...")
    rows: List[Dict] = []

    for sm in stored_models:
        sm_id = sm.get("id")
        sm_group = sm.get("group", "UNKNOWN_GROUP")
        meta = sm.get("modelMetaData", {}) or {}
        sm_name = meta.get("name", sm_id)

        mlcs = fetch_model_mlcs_for_stored_model(base_url, session, sm_id, sm_group) # type: ignore

        for mlc in mlcs:
            proc = mlc.get("processInstance", {}) or {}
            rows.append(
                {
                    "storedModelId": sm_id,
                    "storedModelName": sm_name,
                    "group": sm_group,
                    "mlcId": mlc.get("id"),
                    "processDefinitionKey": proc.get("processDefinitionKey"),
                    "processDefinitionName": proc.get("processDefinitionName"),
                    "processStartTime": proc.get("startTime"),
                    "processEndTime": proc.get("endTime"),
                    "processDurationMs": proc.get("durationInMillis"),
                    "processState": proc.get("state"),
                }
            )

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    logger.info("Exported %d MLC entries to %s", len(df), csv_path)
    return df


def process_and_export_auditrecords(
    stored_models: List[Dict], base_url: str, session: requests.Session, csv_path: str
) -> pd.DataFrame:
    """
    Process current AuditRecords state and export to CSV.
    This captures the BEFORE state before any backfill operations.
    """
    logger.info("Capturing current AuditRecords state (BEFORE backfill)...")
    rows: List[Dict] = []

    for sm in stored_models:
        sm_id = sm.get("id")
        meta = sm.get("modelMetaData", {}) or {}
        sm_name = meta.get("name", sm_id)
        sm_group = sm.get("group", "UNKNOWN_GROUP")

        audit_records = fetch_existing_audit_records(base_url, session, sm_id) # type: ignore

        if not audit_records:
            # Record that no AuditRecords exist
            rows.append(
                {
                    "storedModelId": sm_id,
                    "storedModelName": sm_name,
                    "group": sm_group,
                    "auditRecordId": None,
                    "auditRecordCreatedDate": None,
                    "auditRecordLastModifiedDate": None,
                    "auditRecordCreatedBy": None,
                    "auditRecordLastModifiedBy": None,
                    "modelStageInAuditRecord": None,
                    "recordExists": False,
                }
            )
        else:
            # Record each existing AuditRecord
            for ar in audit_records:
                meta_data = ar.get("metaData", {}) or {}
                rows.append(
                    {
                        "storedModelId": sm_id,
                        "storedModelName": sm_name,
                        "group": sm_group,
                        "auditRecordId": ar.get("id"),
                        "auditRecordCreatedDate": ar.get("createdDate"),
                        "auditRecordLastModifiedDate": ar.get("lastModifiedDate"),
                        "auditRecordCreatedBy": ar.get("createdBy"),
                        "auditRecordLastModifiedBy": ar.get("lastModifiedBy"),
                        "modelStageInAuditRecord": meta_data.get("modelStage"),
                        "recordExists": True,
                    }
                )

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    logger.info("Exported %d AuditRecord entries to %s (current state BEFORE backfill)", len(df), csv_path)
    return df


# ==========================================
# MAIN ORCHESTRATION
# ==========================================

def main() -> None:
    """
    Execute preflight checks before running the backfill operation.

    Process:
    --------
    1. Authenticate to MOC 3.4 
    2. Discover production StoredModels and export to csv
    3. Fetch MLC workflow history for each StoredModel and export to csv
    4. Capture current AuditRecords state BEFORE any modifications and export to csv

    Output Files:
    --------
    - preflight_storedmodels.csv:        Production StoredModels snapshot
    - preflight_mlcs.csv:                MLC workflow execution history
    - preflight_auditrecords_before.csv: Current AuditRecords state BEFORE backfill
    """
    global MOC_ACCESS_TOKEN

    # Create authenticated session
    logger.info("Creating authenticated session to %s...", MOC_BASE_URL)
    session = create_authenticated_session(MOC_BASE_URL, MOC_ACCESS_TOKEN)

    # Step 2: Discover production StoredModels
    stored_models = discover_production_storedmodels(MOC_BASE_URL, session)
    if not stored_models:
        logger.error("No production StoredModels discovered. Aborting.")
        return

    # Export StoredModels
    df_storedmodels = process_and_export_storedmodels(stored_models, PREFLIGHT_STOREDMODELS_CSV)

    # Step 3: Fetch MLC workflow history
    df_mlcs = process_and_export_mlcs(stored_models, MOC_BASE_URL, session, PREFLIGHT_MLCS_CSV)

    # Step 4: Capture current AuditRecords state
    df_auditrecords = process_and_export_auditrecords(stored_models, MOC_BASE_URL, session, PREFLIGHT_AUDITRECORDS_CSV)

    # Summary
    logger.info("=" * 80)
    logger.info("PREFLIGHT CHECK COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info("Exported files:")
    logger.info("  1. %s (%d StoredModels)", PREFLIGHT_STOREDMODELS_CSV, len(df_storedmodels))
    logger.info("  2. %s (%d MLC entries)", PREFLIGHT_MLCS_CSV, len(df_mlcs))
    logger.info("  3. %s (%d AuditRecord entries BEFORE backfill)", PREFLIGHT_AUDITRECORDS_CSV, len(df_auditrecords))
    logger.info("=" * 80)
    logger.info("Next steps:")
    logger.info("  1. Review the exported CSV files to understand current state")
    logger.info("  2. Run: python backfill_storedModel_auditRecords.py")
    logger.info("  3. Compare preflight_auditrecords_before.csv with auditrecord_backfill_results.csv")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
