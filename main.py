from pathlib import Path
import asyncio
import logging

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
import os
from typing import Dict, Optional

import httpx
from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware

import ph  # debe exponer consultar_y_transformar_masiva(ida)

try:
    import tv  # encontrar_abonado_por_nombre(nombre) y obtener_usuario_cic_disponible()
except Exception:
    tv = None

# -----------------------------
# Configuración general
# -----------------------------
BASE_DIR = Path(__file__).parent.resolve()
STATIC_DIR = BASE_DIR / "static"
TPL_DIR = BASE_DIR / "templates"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
TPL_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Clientes PH (Masiva)")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TPL_DIR))

SESSION_SECRET = os.getenv("SESSION_SECRET", "cambia-esta-clave")
READ_USERNAME = os.getenv("READ_USERNAME", "consulta")
READ_PASSWORD = os.getenv("READ_PASSWORD", "consulta123")
WRITE_USERNAME = os.getenv("WRITE_USERNAME", "gestion")
WRITE_PASSWORD = os.getenv("WRITE_PASSWORD", "gestion123")
HEARTBEAT_URL = os.getenv("HEARTBEAT_URL")
if not HEARTBEAT_URL:
    render_url = os.getenv("RENDER_EXTERNAL_URL", "")
    if render_url:
        HEARTBEAT_URL = render_url.rstrip('/') + "/api/ping"
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "240"))
_heartbeat_task: asyncio.Task | None = None

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    max_age=60 * 60 * 8,
    same_site="lax",
)


def _build_accounts() -> list[tuple[str, str, str]]:
    accounts: list[tuple[str, str, str]] = []
    if READ_USERNAME and READ_PASSWORD:
        accounts.append(("read", READ_USERNAME, READ_PASSWORD))
    if WRITE_USERNAME and WRITE_PASSWORD:
        accounts.append(("write", WRITE_USERNAME, WRITE_PASSWORD))
    return accounts


ACCOUNTS = _build_accounts()


def authenticate(username: str, password: str) -> Optional[str]:
    for role, user, pwd in ACCOUNTS:
        if username == user and password == pwd:
            return role
    return None


def require_auth(request: Request) -> Dict[str, str]:
    auth = request.session.get("auth")
    if not auth:
        raise HTTPException(status_code=401, detail="Sesión expirada o inexistente")
    return auth


def require_write(auth: Dict[str, str] = Depends(require_auth)) -> Dict[str, str]:
    if auth.get("role") != "write":
        raise HTTPException(status_code=403, detail="Sin permisos para crear usuarios")
    return auth


async def _heartbeat_loop(url: str, interval: float) -> None:
    if interval <= 0:
        return
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                await client.get(url)
            except Exception as exc:
                logger.debug("heartbeat failed: %s", exc)
            await asyncio.sleep(interval)


@app.on_event("startup")
async def _on_startup():
    global _heartbeat_task
    if HEARTBEAT_URL and HEARTBEAT_INTERVAL > 0:
        _heartbeat_task = asyncio.create_task(_heartbeat_loop(HEARTBEAT_URL, HEARTBEAT_INTERVAL))


@app.on_event("shutdown")
async def _on_shutdown():
    global _heartbeat_task
    if _heartbeat_task:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass
        _heartbeat_task = None


# -----------------------------
# Rutas HTML
# -----------------------------
@app.get("/login", response_class=HTMLResponse)
def login_form(request: Request):
    if request.session.get("auth"):
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    role = authenticate(username.strip(), password.strip())
    if not role:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Usuario o contraseña inválidos"},
            status_code=400,
        )
    request.session["auth"] = {"user": username.strip(), "role": role}
    return RedirectResponse(url="/", status_code=303)


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    auth = request.session.get("auth")
    if not auth:
        return RedirectResponse(url="/login", status_code=303)
    role = auth.get("role", "read")
    username = auth.get("user", "")
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "role": role,
            "username": username,
        },
    )


# -----------------------------
# API
# -----------------------------
@app.get("/api/ping")
def ping():
    return {"ok": True}


@app.get("/api/cliente")
def api_cliente(
    request: Request,
    ida: str = Query(..., description="IDA del cliente"),
    auth: Dict[str, str] = Depends(require_auth),
):
    """
    Devuelve datos del cliente desde Consulta_Masiva_Datos (PH) y
    agrega datos del Sheet:
      - abonado_sheet: str (si matchea)
      - usuario_sheet: str (usuario de la hoja, si existe)
      - cic_sheet: str (CIC desde la hoja, si existe)
      - AltaURL: link para crear usuario (desde .env)
      - UsuarioPropuesto / CICPropuesto: si NO hay usuario en la hoja,
        toma la primera fila con 'Registrado' = 'no' (col D), y devuelve
        Usuario (col C) y CIC (col B).
    """
    ida = (ida or "").strip()
    if not ida:
        raise HTTPException(400, "IDA vacío")

    try:
        data = ph.consultar_y_transformar_masiva(ida)

        match = None
        if tv is not None:
            try:
                match = tv.encontrar_abonado_por_nombre(data.get("Nombre", ""))
            except Exception:
                match = None

        data["ya_tiene_usuario"] = bool(match)
        data["abonado_sheet"] = (match or {}).get("abonado", "")
        data["usuario_sheet"] = (match or {}).get("usuario", "")
        data["cic_sheet"] = (match or {}).get("cic", "")

        data["AltaURL"] = os.getenv("ALTA_URL", "")

        data["UsuarioPropuesto"] = ""
        data["CICPropuesto"] = ""
        if not data["ya_tiene_usuario"] and tv is not None:
            try:
                disp = tv.obtener_usuario_cic_disponible()
                if disp:
                    data["UsuarioPropuesto"] = disp.get("usuario", "")
                    data["CICPropuesto"] = disp.get("cic", "")
                    data["fila_propuesta"] = disp.get("row_index")
            except Exception:
                pass

        return data

    except Exception as e:
        raise HTTPException(500, f"No se pudo consultar {ida}: {e}")


class MarcaPayload(BaseModel):
    usuario: str
    ida: str
    nombre: str
    row_index: int | None = None


@app.post("/api/marcar_registro")
def api_marcar_registro(payload: MarcaPayload, auth: Dict[str, str] = Depends(require_write)):
    if tv is None:
        raise HTTPException(status_code=500, detail="Integración con Google Sheets no disponible")
    try:
        res = tv.marcar_registro_sheet(
            usuario=payload.usuario,
            ida=payload.ida,
            nombre=payload.nombre,
            row_index=payload.row_index,
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
