"""Thin wrapper around Phantom Consulta_Masiva_Datos API.

This module centralises authentication and transforms the raw payload into the
shape expected by the FastAPI endpoints.
"""

from __future__ import annotations

import json
import os
import re
import time
import unicodedata
from collections import OrderedDict
from threading import Lock
from typing import Any, Dict

from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter

load_dotenv()

PH_URL = os.getenv("PH_URL")
PH_USER = os.getenv("PH_USER")
PH_PASS = os.getenv("PH_PASS")

REQUEST_TIMEOUT = float(os.getenv("PH_TIMEOUT_SECONDS", "20"))
CACHE_TTL = float(os.getenv("PH_CACHE_TTL_SECONDS", "180"))
CACHE_MAX = int(os.getenv("PH_CACHE_MAX_ENTRIES", "64"))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "UsuariosGiga/1.0"})
_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
SESSION.mount("http://", _adapter)
SESSION.mount("https://", _adapter)

_TOKEN = {"value": "", "ts": 0.0}
_CACHE_LOCK = Lock()
_CACHE: "OrderedDict[str, tuple[float, Dict[str, Any]]]" = OrderedDict()


def _json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        text = resp.content.decode("utf-8-sig", errors="replace").strip()
        return json.loads(text or "{}")


def _purge_expired(now: float) -> None:
    if CACHE_TTL <= 0:
        _CACHE.clear()
        return
    expired = [key for key, (ts, _) in _CACHE.items() if now - ts > CACHE_TTL]
    for key in expired:
        _CACHE.pop(key, None)


def _cache_get(key: str) -> Dict[str, Any] | None:
    if CACHE_TTL <= 0:
        return None
    now = time.time()
    with _CACHE_LOCK:
        _purge_expired(now)
        item = _CACHE.get(key)
        if not item:
            return None
        _CACHE.move_to_end(key)
        _, data = item
        return dict(data)


def _cache_set(key: str, value: Dict[str, Any]) -> None:
    if CACHE_TTL <= 0:
        return
    now = time.time()
    with _CACHE_LOCK:
        _purge_expired(now)
        _CACHE[key] = (now, dict(value))
        _CACHE.move_to_end(key)
        while len(_CACHE) > CACHE_MAX > 0:
            _CACHE.popitem(last=False)


def _token() -> str:
    if not all([PH_URL, PH_USER, PH_PASS]):
        raise RuntimeError("PH_URL/PH_USER/PH_PASS not configured in .env")
    now = time.time()
    if _TOKEN["value"] and now - _TOKEN["ts"] < 12 * 60:
        return _TOKEN["value"]
    resp = SESSION.get(
        PH_URL,
        params={"action": "autentificar", "api_user": PH_USER, "api_pass": PH_PASS, "JSON": 1},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    token = (_json(resp) or {}).get("token", "")
    if not token:
        raise RuntimeError("Phantom API returned an empty token")
    _TOKEN["value"] = token
    _TOKEN["ts"] = now
    return token


def _req_masiva(payload: Dict[str, Any]) -> Any | None:
    if not PH_URL:
        raise RuntimeError("PH_URL not configured in .env")
    params = {"action": "Consulta_Masiva_Datos", "JSON": 1}
    attempts = (
        ("post", {"params": params, "json": payload, "headers": {"Accept": "application/json"}}),
        (
            "post",
            {
                "params": params,
                "data": payload,
                "headers": {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            },
        ),
        ("get", {"params": {**params, **payload}, "headers": {"Accept": "application/json"}}),
    )
    for method, kwargs in attempts:
        try:
            resp = SESSION.request(method=method, url=PH_URL, timeout=REQUEST_TIMEOUT, **kwargs)
            if resp.status_code == 200:
                return _json(resp)
        except Exception:
            continue
    return None


def _iter_records(data: Any):
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(data, dict):
        for key in ("abonados", "Abonados", "data", "Data", "rows", "items", "result"):
            arr = data.get(key)
            if isinstance(arr, list):
                for item in arr:
                    if isinstance(item, dict):
                        yield item
        if any(isinstance(v, (str, int, float)) for v in data.values()):
            yield data


def consulta_masiva_por_id(ida: int | str) -> Dict[str, Any] | None:
    token = _token()
    ida_int = int(ida)
    payloads = [
        {"token": token, "ID_Desde": ida_int, "ID_Hasta": ida_int},
        {"token": token, "Id_Desde": ida_int, "Id_Hasta": ida_int},
        {"token": token, "IDDesde": ida_int, "IDHasta": ida_int},
        {"token": token, "Desde": ida_int, "Hasta": ida_int},
    ]
    for payload in payloads:
        data = _req_masiva(payload)
        if not data:
            continue
        if isinstance(data, dict) and "code" in data and str(data["code"]) not in {"200", "OK"}:
            continue
        for rec in _iter_records(data):
            rid = str(rec.get("ID") or rec.get("IDA") or "")
            if rid == str(ida_int):
                return rec
        for rec in _iter_records(data):
            return rec
    return None


def _normalize_text(value: str) -> str:
    if value is None:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", str(value)) if unicodedata.category(c) != "Mn"
    ).upper()


_EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)


def _clean_email_text(value: str) -> str:
    if not value:
        return ""
    value = str(value)
    value = value.replace(" at ", "@").replace("[at]", "@").replace("(at)", "@").replace(" arroba ", "@")
    value = value.replace(" dot ", ".").replace("[dot]", ".").replace("(dot)", ".")
    value = value.replace(",", " ").replace(";", " ").replace("|", " ")
    return value


def extraer_mail(record: Dict[str, Any]) -> str:
    candidates = []
    for key in ("Email", "email", "Mail", "MAIL", "E-mail", "UsuarioAutogestion", "Autogestion_User"):
        if key in record and record[key]:
            candidates.append(record[key])
    if not candidates:
        candidates = [v for v in record.values() if isinstance(v, str)]
    for value in candidates:
        match = _EMAIL_RE.search(_clean_email_text(value))
        if match:
            return match.group(0).lower()
    return ""


def _parse_product_flags(productos: str) -> Dict[str, bool]:
    items = [p.strip() for p in (productos or "").split(";") if p.strip()]
    normalized = [_normalize_text(p) for p in items]
    has_tv = any("SERVICIO TV" in p or "BASICO" in p or "BASIC" in p for p in normalized)
    has_hbo = any("HBO" in p for p in normalized)
    has_pf = any("PACK FUTB" in p or "FUTBOL" in p or "DEPORTIVO" in p for p in normalized)
    return {"TV": has_tv, "HBO": has_hbo, "Pack Futbol": has_pf}


def _estado_code(value: str) -> str:
    normalized = _normalize_text(value)
    if normalized.startswith("ACT"):
        return "activo"
    if normalized.startswith("SUS"):
        return "suspendido"
    if normalized.startswith("BAJ"):
        return "baja"
    return "desconocido"


def transformar_desde_masiva(record: Dict[str, Any]) -> Dict[str, Any]:
    nombre_base = record.get("RS") or f"{record.get('Apellido', '')} {record.get('Nombre', '')}"
    nombre_limpio = " ".join(str(nombre_base).replace(",", " ").split())
    dni = record.get("Documento") or record.get("CUIT") or ""
    mail = record.get("Email") or extraer_mail(record)
    iniciales = "".join(word[:1] for word in nombre_limpio.split() if word).lower()
    contrasena = f"{iniciales}{dni}".strip()
    flags = _parse_product_flags(record.get("Television") or record.get("Productos") or "")
    estado_txt = (record.get("Estado") or record.get("estado") or "").strip()
    estado_code = _estado_code(estado_txt)
    return {
        "ID": str(record.get("ID") or record.get("IDA") or ""),
        "Nombre": nombre_limpio,
        "DNI": str(dni) if dni is not None else "",
        "Mail": (mail or "").strip(),
        "Contrasena": contrasena,
        "TV": flags["TV"],
        "HBO": flags["HBO"],
        "Pack Futbol": flags["Pack Futbol"],
        "Estado": estado_txt,
        "EstadoCode": estado_code,
        "_via": "masiva",
    }


def consultar_y_transformar_masiva(ida: int | str) -> Dict[str, Any]:
    ida_int = int(ida)
    cache_key = str(ida_int)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    record = consulta_masiva_por_id(ida_int)
    if not record:
        raise RuntimeError("Consulta_Masiva_Datos did not return data")
    data = transformar_desde_masiva(record)
    _cache_set(cache_key, data)
    return data


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--ida", required=True)
    args = parser.parse_args()
    print(json.dumps(consultar_y_transformar_masiva(args.ida), ensure_ascii=False, indent=2))
