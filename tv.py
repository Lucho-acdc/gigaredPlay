"""Google Sheets helpers for the GigaredPlay workflow."""

import os
import re
import time
import unicodedata
from collections import Counter
from copy import deepcopy
from threading import Lock
from typing import Optional, Dict, Any, List

import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1MS-5SwNBjACZEGie2cOYSu5KLc2YmhmtRfsjis7nuFQ")
WORKSHEET_INDEX = int(os.getenv("WORKSHEET_INDEX", "0"))
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "usuariosgigaredplay-eb5981b62919.json")
SHEET_CACHE_TTL = float(os.getenv("SHEET_CACHE_TTL_SECONDS", "45"))

_sheet_cache_lock = Lock()
_sheet_cache: Dict[str, tuple[float, Any]] = {}


def _cache_get(key: str):
    if SHEET_CACHE_TTL <= 0:
        return None
    now = time.time()
    with _sheet_cache_lock:
        entry = _sheet_cache.get(key)
        if not entry:
            return None
        ts, value = entry
        if now - ts > SHEET_CACHE_TTL:
            _sheet_cache.pop(key, None)
            return None
        return deepcopy(value)


def _cache_set(key: str, value: Any) -> None:
    if SHEET_CACHE_TTL <= 0:
        return
    now = time.time()
    with _sheet_cache_lock:
        _sheet_cache[key] = (now, deepcopy(value))


def _cache_clear() -> None:
    with _sheet_cache_lock:
        _sheet_cache.clear()


# Normalisation helpers

def _norm(value: str) -> str:
    return (value or "").strip()


def _normkey(value: str) -> str:
    if not value:
        return ""
    value = "".join(c for c in unicodedata.normalize("NFD", str(value)) if unicodedata.category(c) != "Mn")
    value = value.lower()
    return re.sub(r"[\s_\-]+", "", value)


def _name_signature(value: str) -> Counter:
    """Return a token multiset for a name (accent insensitive)."""
    if not value:
        return Counter()
    base = "".join(c for c in unicodedata.normalize("NFD", str(value)) if unicodedata.category(c) != "Mn").upper()
    tokens = re.findall(r"[A-Z0-9]+", base)
    return Counter(tokens)


def _open_ws():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    ss = client.open_by_key(SPREADSHEET_ID)
    return ss.get_worksheet(WORKSHEET_INDEX)


def load_data_from_sheet(force_refresh: bool = False) -> List[Dict[str, Any]]:
    """Return sheet rows as a list of dicts using a best effort header selection."""
    values = None if force_refresh else _cache_get("matrix")
    if values is None:
        ws = _open_ws()
        values = ws.get_all_values()
        _cache_set("matrix", values)
    if not values:
        return []

    # Pick a header row within the first five lines (prefer one containing "CIC").
    header_index = 0
    best_nonempty = -1
    for idx, row in enumerate(values[:5]):
        nonempty = sum(1 for cell in row if _norm(cell))
        if any(_norm(cell).upper() == "CIC" for cell in row):
            header_index = idx
            break
        if nonempty > best_nonempty:
            best_nonempty = nonempty
            header_index = idx

    header_raw = values[header_index]

    headers: List[str] = []
    seen: Dict[str, int] = {}
    for j, header in enumerate(header_raw):
        name = _norm(header) or f"col_{j + 1}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        headers.append(name)

    records: List[Dict[str, Any]] = []
    for row in values[header_index + 1 :]:
        if not any(_norm(cell) for cell in row):
            continue
        record = {headers[j]: _norm(row[j]) if j < len(row) else "" for j in range(len(headers))}
        records.append(record)

    _cache_set("records", records)
    return records


def _find_key(row: Dict[str, Any], candidates: List[str]) -> Optional[str]:
    """Return the actual key in a row matching any of the candidate names."""
    if not row:
        return None
    normalised = {_normkey(key): key for key in row.keys()}
    desired = [_normkey(c) for c in candidates]
    for cand in desired:
        if cand in normalised:
            return normalised[cand]
    for cand in desired:
        for k_norm, k_real in normalised.items():
            if k_norm.startswith(cand):
                return k_real
    return None


def _get_abonado_num(row: Dict[str, Any]) -> str:
    key = _find_key(row, ["Gigared", "Abonado", "Numero", "Nro Abonado"])
    if not key:
        return ""
    value = row.get(key, "").strip()
    return value


def encontrar_abonado_por_nombre(nombre_busqueda: str) -> Optional[Dict[str, Any]]:
    """Return sheet metadata for a client whose name tokens match exactly."""
    rows = load_data_from_sheet()
    signature_query = _name_signature(nombre_busqueda)
    for row in rows:
        key_nombre = _find_key(row, ["Nombre", "Razon Social", "Razon_Social", "Cliente", "Titular"])
        if not key_nombre:
            continue
        signature_row = _name_signature(row.get(key_nombre, ""))
        if signature_row == signature_query:
            abonado = _get_abonado_num(row)
            key_cic = _find_key(row, ["CIC"])
            cic = str(row.get(key_cic, "")).strip() if key_cic else ""
            key_usuario = _find_key(row, ["Usuario", "Usuario GP", "Usuario GigaredPlay", "User"])
            usuario = str(row.get(key_usuario, "")).strip() if key_usuario else ""
            return {"abonado": abonado, "row": row, "cic": cic, "usuario": usuario}
    return None


def obtener_usuario_cic_disponible() -> Optional[Dict[str, Any]]:
    """Return the first available user ("Registrado" == "no") with columns B-D."""
    values = _cache_get("matrix")
    if values is None:
        ws = _open_ws()
        values = ws.get_all_values()
        _cache_set("matrix", values)
    if not values or len(values) < 2:
        return None

    headers = values[0]

    def find_col_idx(name: str) -> int:
        target = _normkey(name)
        for index, header in enumerate(headers):
            if _normkey(header) == target:
                return index
        for index, header in enumerate(headers):
            if _normkey(header).startswith(target):
                return index
        return -1

    idx_cic = find_col_idx("CIC")
    idx_user = find_col_idx("Usuario")
    idx_reg = find_col_idx("Registrado")

    if min(idx_cic, idx_user, idx_reg) < 0:
        return None

    for row_index in range(1, len(values)):
        row = values[row_index]
        reg_raw = (row[idx_reg] if idx_reg < len(row) else "").strip().lower()
        if reg_raw in ("no", "n", "false", "0"):
            usuario = (row[idx_user] if idx_user < len(row) else "").strip()
            cic = (row[idx_cic] if idx_cic < len(row) else "").strip()
            if usuario or cic:
                return {"usuario": usuario, "cic": cic, "row_index": row_index + 1}
    return None


def _open_ws_rw():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    ss = client.open_by_key(SPREADSHEET_ID)
    return ss.get_worksheet(WORKSHEET_INDEX)


def marcar_registro_sheet(usuario: str, ida: str, nombre: str, row_index: Optional[int] = None) -> Dict[str, Any]:
    """Mark the sheet as processed for the given user and invalidate caches."""
    ws = _open_ws_rw()

    target_row = None
    if row_index and row_index >= 2:
        target_row = int(row_index)
    else:
        col_usuario = ws.col_values(3)
        usuario_norm = (usuario or "").strip()
        for idx, value in enumerate(col_usuario, start=1):
            if idx == 1:
                continue
            if (value or "").strip() == usuario_norm:
                target_row = idx
                break

    if not target_row:
        raise ValueError(f"No se encontro la fila del usuario '{usuario}'")

    ws.update_cell(target_row, 4, "Mail")
    ws.update_cell(target_row, 6, str(ida or ""))
    ws.update_cell(target_row, 7, str(nombre or ""))

    a1_range = f"A{target_row}:J{target_row}"
    ws.format(a1_range, {"textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}}})

    _cache_clear()
    return {"ok": True, "row": target_row, "usuario": usuario, "ida": ida, "nombre": nombre}


if __name__ == "__main__":
    rows = load_data_from_sheet()
    print(f"Filas leidas: {len(rows)}")
    disp = obtener_usuario_cic_disponible()
    print("Primer disponible:", disp)
