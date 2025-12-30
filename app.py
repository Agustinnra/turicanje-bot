import os
import json
import hmac
import hashlib
import re
import random
import time
import math
from typing import Dict, Optional, Any, List, Tuple
from datetime import datetime, time as dt_time


import pytz
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
import httpx
import psycopg
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

from datetime import datetime
import pytz

DAY_MAP = {
    0: ("mon_open", "mon_close"),
    1: ("tue_open", "tue_close"),
    2: ("wed_open", "wed_close"),
    3: ("thu_open", "thu_close"),
    4: ("fri_open", "fri_close"),
    5: ("sat_open", "sat_close"),
    6: ("sun_open", "sun_close"),
}

def get_today_hours_filter() -> str:
    """
    Retorna la condición SQL para filtrar lugares que tengan horarios HOY.
    Ejemplo: Si hoy es lunes → "mon_open IS NOT NULL AND mon_close IS NOT NULL"
    """
    import datetime
    import pytz
    
    # Obtener día actual en México (timezone por defecto)
    tz = pytz.timezone("America/Mexico_City")
    now = datetime.datetime.now(tz)
    weekday = now.weekday()  # 0=lunes, 6=domingo
    
    open_col, close_col = DAY_MAP[weekday]
    
    return f"{open_col} IS NOT NULL AND {close_col} IS NOT NULL"

def is_open_now_by_day(place: dict) -> bool:
    """
    Determina si un lugar está abierto AHORA usando las columnas individuales de horarios.
    Soporta:
    - Formatos: "HH:MM:SS", "HH:MM", "H:MM:SS", "H:MM"
    - Horarios que cruzan medianoche (ej: 22:00 - 02:00)
    - Zona horaria del lugar
    - Verifica el día anterior si son horas muy tempranas (antes de las 6 AM)
    """
    place_name = place.get('name', 'UNKNOWN')
    place_id = place.get('id', 'UNKNOWN')
    
    tz_name = place.get("timezone") or "America/Mexico_City"
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = pytz.timezone("America/Mexico_City")

    now = datetime.now(tz)
    weekday = now.weekday()
    
    # DEBUG: Log de entrada
    print(f"[OPEN-CHECK-DEBUG] Verificando {place_id} - {place_name}")

    def parse_time(time_str):
        """Helper para parsear tiempos en múltiples formatos"""
        time_str = str(time_str).strip()
        for fmt in ["%H:%M:%S", "%H:%M"]:
            try:
                return datetime.strptime(time_str, fmt).time()
            except ValueError:
                continue
        raise ValueError(f"No se pudo parsear: {time_str}")

    def check_day(day_index):
        """Verifica si está abierto en un día específico"""
        open_key, close_key = DAY_MAP[day_index]
        open_time = place.get(open_key)
        close_time = place.get(close_key)
        
        # DEBUG: Log de horarios
        print(f"[OPEN-CHECK-DEBUG] {place_name} - {open_key}={open_time}, {close_key}={close_time}")

        if not open_time or not close_time:
            print(f"[OPEN-CHECK-DEBUG] {place_name} - Sin horarios para {open_key}/{close_key}")
            return False

        try:
            open_t = parse_time(open_time)
            close_t = parse_time(close_time)
            
            open_dt = tz.localize(datetime.combine(now.date(), open_t))
            close_dt = tz.localize(datetime.combine(now.date(), close_t))

            # Si cierra "antes" de abrir, cruza medianoche
            if close_dt <= open_dt:
                close_dt = close_dt.replace(day=close_dt.day + 1)

            is_open = open_dt <= now <= close_dt
            print(f"[OPEN-CHECK-DEBUG] {place_name} - is_open={is_open}, now={now.time()}, open={open_t}, close={close_t}")
            return is_open
        except Exception as e:
            print(f"[OPEN-CHECK-DEBUG] {place_name} - ERROR: {e}")
            return False

    # Verificar día actual
    if check_day(weekday):
        print(f"[OPEN-CHECK] ✅ {place_name} ABIERTO (día actual)")
        return True

    # Si son horas muy tempranas (antes de las 6 AM), verificar día anterior
    # Esto cubre el caso: Sábado 22:00 - Domingo 3:00
    if now.hour < 6:
        prev_day = (weekday - 1) % 7
        if check_day(prev_day):
            print(f"[OPEN-CHECK] ✅ {place_name} ABIERTO (horario del día anterior que cruza medianoche)")
            return True

    print(f"[OPEN-CHECK] ❌ {place_name} CERRADO")
    return False

def get_hours_status_from_columns(place: dict) -> Tuple[bool, str, bool]:
    """
    Calcula el estado de horarios usando las columnas individuales (mon_open, tue_open, etc.)
    
    Maneja:
    - Horarios normales
    - Horarios que cruzan medianoche (ej: 22:00 - 02:00)
    - Verifica día anterior si son horas tempranas (antes de 6 AM)
    
    Returns:
        Tuple[bool, str, bool]: (is_open, hours_text, has_hours)
        - is_open: True si está abierto ahora
        - hours_text: Texto descriptivo ("hasta 22:00", "abre a las 09:00", etc.)
        - has_hours: True si tiene información de horarios en la BD
    """
    tz_name = place.get("timezone") or "America/Mexico_City"
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz = pytz.timezone("America/Mexico_City")

    now = datetime.now(tz)
    weekday = now.weekday()

    def parse_time(time_str):
        """Helper para parsear tiempos"""
        time_str = str(time_str).strip()
        for fmt in ["%H:%M:%S", "%H:%M"]:
            try:
                return datetime.strptime(time_str, fmt).time()
            except ValueError:
                continue
        raise ValueError(f"No se pudo parsear: {time_str}")

    def check_day_status(day_index):
        """Verifica el estado de un día específico"""
        open_key, close_key = DAY_MAP[day_index]
        open_time = place.get(open_key)
        close_time = place.get(close_key)

        if not open_time or not close_time:
            return (False, "", False)

        try:
            open_t = parse_time(open_time)
            close_t = parse_time(close_time)
            
            open_dt = tz.localize(datetime.combine(now.date(), open_t))
            close_dt = tz.localize(datetime.combine(now.date(), close_t))

            # Manejar horarios que cruzan medianoche
            if close_dt <= open_dt:
                close_dt = close_dt.replace(day=close_dt.day + 1)

            is_open = open_dt <= now <= close_dt
            
            close_formatted = close_t.strftime("%H:%M")
            open_formatted = open_t.strftime("%H:%M")
            
            if is_open:
                return (True, f"hasta {close_formatted}", True)
            else:
                return (False, f"abre a las {open_formatted}", True)
            
        except Exception:
            return (False, "", False)

    # 1. Verificar día actual
    is_open, hours_text, has_hours = check_day_status(weekday)
    if is_open:
        return (True, hours_text, has_hours)

    # 2. Si son horas muy tempranas (antes de 6 AM), verificar día anterior
    # Esto cubre: Sábado 22:00 - Domingo 3:00 AM
    if now.hour < 6:
        prev_day = (weekday - 1) % 7
        prev_is_open, prev_hours_text, prev_has_hours = check_day_status(prev_day)
        if prev_is_open:
            return (True, prev_hours_text, prev_has_hours)

    # 3. Si no está abierto, retornar info del día actual
    return (False, hours_text if has_hours else "", has_hours)

# ================= ENV =================
load_dotenv()

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "verifica_turicanje")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID", "")
APP_SECRET = os.getenv("APP_SECRET", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TZ = os.getenv("TZ", "America/Mexico_City")
SEND_VIA_WHATSAPP = os.getenv("SEND_VIA_WHATSAPP", "true").lower() == "true"

# Base de datos
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Configuración
IDLE_RESET_SECONDS = int(os.getenv("IDLE_RESET_SECONDS", "120"))  # 2 minutos
MAX_SUGGESTIONS = 3  # FIJO: Siempre 3 opciones

# Configuración DUAL (DEV + PROD)
DEV_PHONE_NUMBER_ID = "816732738189248"
DEV_WHATSAPP_TOKEN = os.getenv("DEV_WHATSAPP_TOKEN", "")

PROD_PHONE_NUMBER_ID = "840950589099677"
PROD_WHATSAPP_TOKEN = os.getenv("PROD_WHATSAPP_TOKEN", "")

ENVIRONMENT = os.getenv("ENVIRONMENT", "production")

def get_environment_config(phone_number_id: str) -> dict:
    """Determina si el mensaje viene de DEV o PROD"""
    if phone_number_id == DEV_PHONE_NUMBER_ID:
        return {
            "env": "DEV",
            "phone_number_id": DEV_PHONE_NUMBER_ID,
            "whatsapp_token": DEV_WHATSAPP_TOKEN or WHATSAPP_TOKEN,
            "prefix": "[DEV]"
        }
    elif phone_number_id == PROD_PHONE_NUMBER_ID:
        return {
            "env": "PROD", 
            "phone_number_id": PROD_PHONE_NUMBER_ID,
            "whatsapp_token": PROD_WHATSAPP_TOKEN or WHATSAPP_TOKEN,
            "prefix": "[PROD]"
        }
    else:
        return {
            "env": "UNKNOWN",
            "phone_number_id": phone_number_id,
            "whatsapp_token": WHATSAPP_TOKEN,
            "prefix": "[?]"
        }


# ================= APP =================
app = FastAPI(title="Turicanje Bot", version="1.0.0")

# Pool de conexiones a DB
_pool: Optional[ConnectionPool] = None

def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        conninfo = (
            f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
            f"user={DB_USER} password={DB_PASSWORD} sslmode=require"
        )
        _pool = ConnectionPool(
            conninfo=conninfo,
            min_size=0,
            max_size=8,
            kwargs={"autocommit": True},
            open=False,
        )
    try:
        if getattr(_pool, "closed", True):
            _pool.open()
    except Exception:
        _pool.open()
    return _pool

@app.on_event("startup")
async def startup():
    try:
        get_pool().open()
        with get_pool().connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
        print("[DB] Pool conectado correctamente")
    except Exception as e:
        print(f"[DB] Error conectando: {e}")

@app.on_event("shutdown")
async def shutdown():
    try:
        if _pool and not getattr(_pool, "closed", True):
            _pool.close()
            print("[DB] Pool cerrado")
    except Exception as e:
        print(f"[DB] Error cerrando pool: {e}")

# ================= UTILIDADES =================
def local_now() -> datetime:
    return datetime.now(pytz.timezone(TZ))

def verify_signature(request: Request, body: bytes) -> bool:
    if not APP_SECRET:
        return True
    sig = request.headers.get("X-Hub-Signature-256", "")
    if not sig.startswith("sha256="):
        return True
    digest = hmac.new(APP_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={digest}", sig)

def format_distance(meters: float) -> str:
    if meters < 1000:
        return f"{int(meters)} m"
    else:
        return f"{meters/1000:.1f} km"

from datetime import datetime
import pytz

def compute_open_status(place: dict) -> dict:
    """
    Calcula si un lugar está abierto ahora, usando mon_open, mon_close, etc.
    Retorna flags que el bot puede usar para ranking y respuestas.
    """

    tz_name = place.get("timezone") or "America/Mexico_City"
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)

    weekday = now.weekday()  # 0=mon ... 6=sun
    day_map = {
        0: "mon",
        1: "tue",
        2: "wed",
        3: "thu",
        4: "fri",
        5: "sat",
        6: "sun",
    }

    day = day_map[weekday]
    open_key = f"{day}_open"
    close_key = f"{day}_close"

    open_time = place.get(open_key)
    close_time = place.get(close_key)

    if not open_time or not close_time:
        return {
            "is_open_now": False,
            "has_today_hours": False,
            "today_open": None,
            "today_close": None,
        }

    try:
        open_dt = tz.localize(datetime.combine(now.date(), datetime.strptime(open_time, "%H:%M:%S").time()))
        close_dt = tz.localize(datetime.combine(now.date(), datetime.strptime(close_time, "%H:%M:%S").time()))
    except Exception:
        return {
            "is_open_now": False,
            "has_today_hours": False,
            "today_open": open_time,
            "today_close": close_time,
        }

    is_open = open_dt <= now <= close_dt

    return {
        "is_open_now": is_open,
        "has_today_hours": True,
        "today_open": open_time,
        "today_close": close_time,
    }


# Agregar estas funciones a tu app.py después de la función format_distance

def is_place_open(hours: dict) -> Tuple[bool, str]:
    """
    Verifica si un lugar está abierto basado en sus horarios.
    MANEJA CORRECTAMENTE: Horarios que cruzan medianoche (ej: 22:00-02:00)
    Retorna: (está_abierto, próximo_horario)
    """
    if not hours:
        return (False, "horario no disponible")

    
    try:
        now = local_now()
        
        # ✅ FIX: Usar weekday() que es independiente del locale
        days_order = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        current_day = days_order[now.weekday()]
        current_time_str = now.strftime('%H:%M')
        current_time_obj = now.time()  # Objeto time para comparación
        
        print(f"[HOURS-CHECK] Día: {current_day}, Hora actual: {current_time_str}")
        
        # Obtener horarios del día actual
        day_hours = hours.get(current_day, [])
        
        if day_hours:
            print(f"[HOURS-CHECK] Horarios de {current_day}: {day_hours}")
        
        # ✅ Verificar si está abierto AHORA
        if isinstance(day_hours, list):
            for schedule in day_hours:
                if isinstance(schedule, list) and len(schedule) >= 2:
                    open_str = schedule[0]
                    close_str = schedule[1]
                    
                    # Convertir a objetos time para comparación correcta
                    try:
                        open_parts = open_str.split(":")
                        close_parts = close_str.split(":")
                        
                        open_time = dt_time(int(open_parts[0]), int(open_parts[1]))
                        close_time = dt_time(int(close_parts[0]), int(close_parts[1]))

                        
                        print(f"[HOURS-CHECK] Verificando intervalo: {open_str}-{close_str}")
                        
                        # CASO 1: Horario normal (no cruza medianoche)
                        # Ejemplo: 08:00 - 20:00
                        if open_time < close_time:
                            if open_time <= current_time_obj <= close_time:
                                print(f"[HOURS-CHECK] ✅ ABIERTO (horario normal)")
                                return (True, f"hasta {close_str}")
                        
                        # CASO 2: Horario que cruza medianoche
                        # Ejemplo: 22:00 - 02:00
                        else:
                            if current_time_obj >= open_time or current_time_obj <= close_time:
                                print(f"[HOURS-CHECK] ✅ ABIERTO (cruza medianoche)")
                                return (True, f"hasta {close_str}")
                    
                    except (ValueError, IndexError) as e:
                        print(f"[HOURS-CHECK] Error parseando horario: {e}")
                        continue
        
        print(f"[HOURS-CHECK] ❌ CERRADO ahora")
        
        # ✅ Si no está abierto, buscar PRÓXIMA apertura
        
        # 1. Buscar si abre más tarde HOY
        if isinstance(day_hours, list):
            for schedule in day_hours:
                if isinstance(schedule, list) and len(schedule) >= 2:
                    open_str = schedule[0]
                    try:
                        open_parts = open_str.split(":")
                        open_time = dt_time(int(open_parts[0]), int(open_parts[1]))
                        
                        if open_time > current_time_obj:
                            return (False, f"abre a las {open_str}")
                    except:
                        continue
        
        # 2. Buscar próximo día que abre
        current_idx = now.weekday()
        day_names_es = {
            'mon': 'lunes', 'tue': 'martes', 'wed': 'miércoles',
            'thu': 'jueves', 'fri': 'viernes', 'sat': 'sábado', 'sun': 'domingo'
        }
        
        for i in range(1, 8):
            next_idx = (current_idx + i) % 7
            next_day = days_order[next_idx]
            next_hours = hours.get(next_day, [])
            
            if isinstance(next_hours, list) and next_hours:
                first_schedule = next_hours[0]
                if isinstance(first_schedule, list) and len(first_schedule) >= 2:
                    open_time = first_schedule[0]
                    day_name = day_names_es.get(next_day, next_day)
                    return (False, f"abre {day_name} a las {open_time}")
        
        # Si no encontramos próxima apertura, retornar cerrado sin info
        return (False, "")
    
    except Exception as e:
        print(f"[ERROR] is_place_open: {e}")
        import traceback
        traceback.print_exc()
        return (True, "")  # En caso de error, asumimos abierto
    
    return (True, "")

# ================= NOMBRES ALEATORIOS =================
NOMBRES_SPANISH = [
    "Ana", "Carlos", "María", "Luis", "Carmen", "José", "Isabella", "Diego",
    "Sofía", "Miguel", "Valentina", "Alejandro", "Camila", "Roberto", "Lucía",
    "Fernando", "Gabriela", "Ricardo", "Natalia", "Andrés", "Elena", "Pablo",
    "Daniela", "Javier", "Adriana", "Manuel", "Patricia", "Francisco", "Mónica"
]

NOMBRES_ENGLISH = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Oliver", "Charlotte", "Elijah",
    "Amelia", "William", "Sophia", "James", "Isabella", "Benjamin", "Mia",
    "Lucas", "Evelyn", "Henry", "Harper", "Alexander", "Camila", "Mason",
    "Gianna", "Michael", "Abigail", "Ethan", "Luna", "Daniel", "Ella"
]

def get_random_name() -> str:
    """Siempre retorna un nombre en español"""
    return random.choice(NOMBRES_SPANISH)

# ================= DETECCIÓN DE IDIOMA =================
def is_spanish(text: str) -> bool:
    """
    Detecta si el texto está en español.
    Retorna True si es español, False si no lo es.
    """
    if not text:
        return True  # Por defecto asumimos español
    
    text_lower = text.lower().strip()
    
    # Palabras comunes en español
    spanish_words = [
        'hola', 'buenos', 'buenas', 'saludos', 'gracias', 'por', 'favor',
        'qué', 'que', 'cómo', 'como', 'dónde', 'donde', 'cuándo', 'cuando',
        'quién', 'quien', 'cuál', 'cual', 'antojo', 'antoja', 'hambre',
        'comer', 'comida', 'restaurante', 'lugar', 'cerca', 'aquí', 'sí', 'si',
        'también', 'tambien', 'muy', 'más', 'mas', 'café', 'pizza', 'tacos',
        'quiero', 'quiere', 'quieres', 'queremos', 'quieren', 'busco', 'busca',
        'necesito', 'necesita', 'tengo', 'tiene', 'tienes', 'ganas', 'gana',
        'me', 'te', 'se', 'le', 'nos', 'les', 'del', 'de', 'la', 'el', 'un', 'una'
    ]
    
    # Palabras comunes en inglés (para detectar cuando NO es español)
    english_words = [
        'hello', 'hi', 'hey', 'good', 'morning', 'afternoon', 'evening',
        'thanks', 'thank', 'you', 'please', 'what', 'how', 'where',
        'when', 'who', 'which', 'craving', 'hungry', 'food', 'eat',
        'restaurant', 'place', 'near', 'here', 'delivery', 'order', 'yes',
        'the', 'a', 'an', 'this', 'that', 'my', 'your', 'want', 'need'
    ]
    
    # Caracteres específicos del español
    has_spanish_chars = bool(re.search(r'[ñáéíóúüÀ-ÿ¡¿]', text))
    
    # Si tiene caracteres españoles, definitivamente es español
    if has_spanish_chars:
        return True
    
    # Contar palabras en cada idioma
    words = re.findall(r'\b\w+\b', text_lower)
    spanish_score = sum(1 for word in words if word in spanish_words)
    english_score = sum(1 for word in words if word in english_words)
    
    print(f"[LANG-DETECT] '{text}' -> ES:{spanish_score}, EN:{english_score}, chars:{has_spanish_chars}")
    
    # Si tiene más palabras en inglés que en español, probablemente NO es español
    if english_score > spanish_score and english_score > 0:
        return False
    
    # En caso de duda o empate, asumimos que es español
    return True

def is_greeting(text: str) -> bool:
    if not text:
        return True
    
    text_lower = text.lower().strip()
    greeting_patterns = [
        r'^\s*[¡!]*\s*(hola|hello|hi|hey|buenas|buenos)\s*[¡!]*\s*$',
        r'^\s*(que\s*tal|qué\s*tal|how\s*are|whats\s*up|what\s*up)',
        r'^\s*(good\s*(morning|afternoon|evening)|buenas\s*(tardes|noches)|buenos\s*días)'
    ]
    
    return any(re.search(pattern, text_lower) for pattern in greeting_patterns)

# ================= WHATSAPP =================
async def send_whatsapp_message(to: str, message: str, phone_number_id: str = None):
    # Determinar configuración
    if phone_number_id:
        config = get_environment_config(phone_number_id)
    else:
        config = {
            "env": ENVIRONMENT.upper(),
            "phone_number_id": PHONE_NUMBER_ID,
            "whatsapp_token": WHATSAPP_TOKEN,
            "prefix": f"[{ENVIRONMENT.upper()}]"
        }
    
    if not SEND_VIA_WHATSAPP:
        print(f"\n{config['prefix']} [DRY-RUN] Mensaje a {to}:")
        print(f"{message}\n")
        return
    
    if not config["whatsapp_token"]:
        print(f"{config['prefix']} [ERROR] Falta token")
        return
    
    url = f"https://graph.facebook.com/v20.0/{config['phone_number_id']}/messages"
    headers = {"Authorization": f"Bearer {config['whatsapp_token']}"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": message[:4096]}
    }
    
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(url, json=data, headers=headers)
            if response.status_code >= 300:
                print(f"{config['prefix']} [ERROR] WhatsApp API: {response.status_code} - {response.text}")
            else:
                print(f"{config['prefix']} [OK] Mensaje enviado a {to}")
    except Exception as e:
        print(f"{config['prefix']} [ERROR] Enviando mensaje: {e}")

async def send_whatsapp_image(to: str, image_url: str, caption: Optional[str] = None, phone_number_id: str = None):
    """
    Envía imagen usando el token correcto según el phone_number_id
    """
    # Determinar configuración
    if phone_number_id:
        config = get_environment_config(phone_number_id)
    else:
        config = {
            "env": ENVIRONMENT.upper(),
            "phone_number_id": PHONE_NUMBER_ID,
            "whatsapp_token": WHATSAPP_TOKEN,
            "prefix": f"[{ENVIRONMENT.upper()}]"
        }
    
    if not SEND_VIA_WHATSAPP:
        print(f"\n{config['prefix']} [DRY-RUN] Imagen a {to}: {image_url}")
        if caption:
            print(f"Caption: {caption}")
        return
    
    if not config["whatsapp_token"]:
        print(f"{config['prefix']} [ERROR] Falta token de WhatsApp para imagen")
        return
    
    url = f"https://graph.facebook.com/v20.0/{config['phone_number_id']}/messages"
    headers = {"Authorization": f"Bearer {config['whatsapp_token']}"}
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "image",
        "image": {"link": image_url}
    }
    
    if caption:
        data["image"]["caption"] = caption[:1024]
    
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(url, json=data, headers=headers)
            if response.status_code >= 300:
                print(f"{config['prefix']} [ERROR] WhatsApp Image API: {response.status_code} - {response.text}")
            else:
                print(f"{config['prefix']} [OK] Imagen enviada a {to}")
    except Exception as e:
        print(f"{config['prefix']} [ERROR] Enviando imagen: {e}")

# ================= GESTIÓN DE USUARIOS =================
user_sessions = {}

def reset_user_session(wa_id: str):
    if wa_id in user_sessions:
        del user_sessions[wa_id]
    print(f"[SESSION] Reset completo para usuario {wa_id}")

def get_or_create_user_session(wa_id: str) -> Dict[str, Any]:
    """Crea o recupera sesión de usuario. Siempre usa español."""
    current_time = time.time()
    
    if wa_id in user_sessions:
        session = user_sessions[wa_id]
        time_diff = current_time - session.get("last_seen", 0)
        
        if time_diff < IDLE_RESET_SECONDS:
            session["last_seen"] = current_time
            return session
        else:
            print(f"[SESSION] Sesión expirada para {wa_id} ({time_diff:.1f}s)")
            reset_user_session(wa_id)
    
    name = get_random_name()  # ✅ Siempre usa nombres en español
    session = {
        "name": name,
        "language": "es",  # ✅ SIEMPRE ESPAÑOL
        "last_seen": current_time,
        "is_new": True,
        "last_search": {},
        "last_results": [],
        "user_location": None
    }
    user_sessions[wa_id] = session
    print(f"[SESSION] Nueva sesión: {wa_id} -> {name} (es)")
    return session

# ================= IA: EXTRACCIÓN DE INTENCIÓN =================
async def expand_search_terms_with_ai(craving: str, language: str, wa_id: str) -> List[str]:
    """
    Expande términos de búsqueda de manera CONSERVADORA.
    Solo incluye sinónimos muy cercanos o variaciones del mismo platillo.
    """
    if not OPENAI_API_KEY:
        return [craving]
    
    try:
        system_prompt = """Eres un experto en comida mexicana. Te dan UNA palabra de comida y debes generar SOLO sinónimos DIRECTOS o variaciones del MISMO PLATILLO.

REGLAS ESTRICTAS:
- Solo expande a variaciones del mismo platillo (ej: "barbacoa" → "barbacoa de borrego", "barbacoa de res")
- NUNCA incluyas platillos diferentes aunque sean similares
- NUNCA incluyas ingredientes genéricos (ej: "carne", "pollo")
- NUNCA incluyas métodos de preparación genéricos (ej: "al vapor", "deshebrada")
- Máximo 4 términos en total

Ejemplos BUENOS:
- "barbacoa" → "barbacoa, barbacoa de borrego, barbacoa de res"
- "tacos" → "tacos, taco"
- "cochinita" → "cochinita, cochinita pibil"

Ejemplos MALOS (NO hacer):
- "barbacoa" → "barbacoa, pibil, carnitas, carne" ❌ (pibil es diferente)
- "tacos" → "tacos, quesadillas, tortas" ❌ (son platillos diferentes)

Responde SOLO una lista separada por comas, sin explicaciones."""
        
        user_prompt = f"Expand de forma conservadora: {craving}"
        
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "gpt-4o-mini",
                    "temperature": 0.1,  # ✅ Más bajo para ser más conservador
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "max_tokens": 40  # ✅ Menos tokens = menos expansión
                }
            )
        
        if response.status_code == 200:
            data = response.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            
            if content:
                terms = [term.strip().lower() for term in content.split(",") if term.strip()]
                terms = [craving.lower()] + [t for t in terms if t != craving.lower()]
                print(f"[AI-EXPAND] {wa_id}: '{craving}' -> {terms}")
                return terms[:4]  # ✅ Máximo 4 términos
        
        return [craving]
        
    except Exception as e:
        print(f"[AI-EXPAND] {wa_id}: Error: {e}")
        return [craving]

async def extract_intent_with_ai(text: str, language: str, name: str, wa_id: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        print(f"[AI-INTENT] {wa_id}: Sin API key, usando fallback")
        return {"intent": "unknown", "craving": None, "needs_location": False, "business_name": None}
    
    try:
        if language == "es":
            system_prompt = f"""Eres {name}, analizas mensajes para extraer qué quiere comer/probar el usuario.
NUNCA inventes comida que no mencionó. Si no menciona comida específica, craving es null.
Si menciona el NOMBRE ESPECÍFICO de un restaurante/negocio (ej: "Starbucks", "McDonald's", "Domino's"), extráelo en business_name.
Responde SOLO en JSON con: {{"intent": "greeting|search|business_search|other", "craving": "texto exacto o null", "needs_location": true/false, "business_name": "nombre exacto o null"}}

Intents:
- greeting: saludos iniciales  
- business_search: busca un restaurante/negocio específico por nombre
- search: busca comida/restaurante por tipo de comida
- other: todo lo demás

needs_location solo es true si pidió "cerca", "aquí cerca", etc.
business_name solo tiene valor si mencionó un nombre específico de negocio."""
        else:
            system_prompt = f"""You are {name}, you analyze messages to extract what the user wants to eat/try.
NEVER invent food they didn't mention. If no specific food mentioned, craving is null.
If they mention a SPECIFIC restaurant/business NAME (e.g., "Starbucks", "McDonald's", "Domino's"), extract it in business_name.
Respond ONLY in JSON: {{"intent": "greeting|search|business_search|other", "craving": "exact text or null", "needs_location": true/false, "business_name": "exact name or null"}}

Intents:
- greeting: initial greetings
- business_search: looking for a specific restaurant/business by name
- search: looking for specific food/restaurant by food type
- other: everything else

needs_location only true if they asked for "nearby", "close", etc.
business_name only has value if they mentioned a specific business name."""
        
        user_prompt = f"Analyze this message: '{text}'"
        
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "gpt-4o-mini",
                    "temperature": 0.1,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "max_tokens": 100
                }
            )
        
        if response.status_code == 200:
            data = response.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            
            # ✅ FIX: Limpiar markdown backticks y texto extra antes de parsear
            content = content.strip()
            # Remover ```json y ``` si existen
            if content.startswith("```json"):
                content = content[7:]  # Remover ```json
            elif content.startswith("```"):
                content = content[3:]  # Remover ```
            if content.endswith("```"):
                content = content[:-3]  # Remover ```
            content = content.strip()
            
            result = json.loads(content)
            intent = result.get("intent", "other")
            craving = result.get("craving")
            needs_location = result.get("needs_location", False)
            business_name = result.get("business_name")
            
            if intent not in ["greeting", "search", "business_search", "other"]:
                intent = "other"
            
            if craving and isinstance(craving, str):
                craving = craving.strip()
                if not craving or craving.lower() in ["null", "none", ""]:
                    craving = None
            else:
                craving = None
            
            if business_name and isinstance(business_name, str):
                business_name = business_name.strip()
                if not business_name or business_name.lower() in ["null", "none", ""]:
                    business_name = None
            else:
                business_name = None
            
            print(f"[AI-INTENT] {wa_id}: intent={intent}, craving={craving}, business_name={business_name}, needs_location={needs_location}")
            
            return {
                "intent": intent,
                "craving": craving, 
                "needs_location": bool(needs_location),
                "business_name": business_name
            }
    
    except Exception as e:
        print(f"[AI-INTENT] {wa_id}: Error: {e}")
    
    fallback_intent = "greeting" if is_greeting(text) else "other"
    print(f"[AI-INTENT] {wa_id}: Fallback -> {fallback_intent}")
    return {
        "intent": fallback_intent,
        "craving": None,
        "needs_location": False,
        "business_name": None
    }


# ================= BASE DE DATOS: NUEVO ORDEN =================

def search_place_by_name(business_name: str) -> Optional[Dict[str, Any]]:
    """
    Busca un negocio específico por nombre exacto o similar
    Retorna el primer resultado que coincida
    """
    if not business_name:
        return None
    
    try:
        # Búsqueda por similitud de nombre (case-insensitive)
        sql = """
        SELECT id, name, category, products, priority, cashback, hours, 
               address, phone, url_order, imagen_url, url_extra, afiliado,
               lat, lng, timezone, delivery,
               mon_open, mon_close, tue_open, tue_close, wed_open, wed_close,
               thu_open, thu_close, fri_open, fri_close, sat_open, sat_close,
               sun_open, sun_close
        FROM public.places 
        WHERE LOWER(name) LIKE %(search_pattern)s
        ORDER BY 
            CASE WHEN LOWER(name) = %(exact_match)s THEN 0 ELSE 1 END,
            LENGTH(name) ASC
        LIMIT 1;
        """
        
        search_pattern = f"%{business_name.lower()}%"
        exact_match = business_name.lower()
        
        params = {
            "search_pattern": search_pattern,
            "exact_match": exact_match
        }
        
        print(f"[DB-SEARCH-NAME] Buscando negocio: '{business_name}'")
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            
            if row:
                place = dict(row)
                place["products"] = list(place.get("products") or [])
                place["hours"] = dict(place.get("hours") or {})
                print(f"[DB-SEARCH-NAME] Encontrado: {place['name']}")
                return place
            else:
                print(f"[DB-SEARCH-NAME] No encontrado: '{business_name}'")
                return None
            
    except Exception as e:
        print(f"[DB-SEARCH-NAME] Error: {e}")
        return None

def search_places_without_location(craving: str, limit: int = 10) -> List[Dict[str, Any]]:
    """NUEVO ORDEN: producto -> afiliado -> prioridad -> id
    FASE 2: Solo muestra lugares con horarios del día actual"""
    if not craving:
        return []
    
    # ✅ FASE 2: Obtener filtro de horarios del día
    today_filter = get_today_hours_filter()
    
    try:
        sql = f"""
        SELECT id, name, category, products, priority, cashback, hours, 
               address, phone, url_order, imagen_url, url_extra, afiliado,
               lat, lng, timezone, delivery,
               mon_open, mon_close, tue_open, tue_close, wed_open, wed_close,
               thu_open, thu_close, fri_open, fri_close, sat_open, sat_close,
               sun_open, sun_close
        FROM public.places 
        WHERE EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(categories) as item
            WHERE LOWER(item) LIKE ANY(%(search_patterns)s)
        )
        AND {today_filter}
        ORDER BY 
            (SELECT COUNT(*) FROM jsonb_array_elements_text(categories) as item
             WHERE LOWER(item) LIKE ANY(%(search_patterns)s)) DESC,
            CASE WHEN afiliado = true THEN 1 ELSE 0 END DESC,
            priority DESC,
            id ASC
        LIMIT %(limit)s;
        """
        
        # Crear patrones de búsqueda para el término original
        search_patterns = [f"%{craving.lower()}%"]
        
        params = {
            "search_patterns": search_patterns,
            "limit": limit
        }
        
        print(f"[DB-SEARCH] FASE 4: Buscando '{craving}' en categories (SEO interno)")
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            
            results = []
            for row in rows:
                place = dict(row)
                place["products"] = list(place.get("products") or [])
                place["is_open_now"] = is_open_now_by_day(place)

                results.append(place)
            
            print(f"[DB-SEARCH] Sin ubicación: {len(results)} resultados")
            return results
            
    except Exception as e:
        print(f"[DB-SEARCH] Error: {e}")
        return []

async def search_places_without_location_ai(craving: str, language: str, wa_id: str, limit: int = 10) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Búsqueda en DOS ETAPAS:
    1. Busca término exacto primero
    2. Si no encuentra nada, busca con expansión de IA
    
    Retorna: (resultados, used_expansion)
    - used_expansion=False si encontró con término exacto
    - used_expansion=True si tuvo que usar expansión
    """
    if not craving:
        return [], False
    
    # ETAPA 1: Buscar término EXACTO primero
    print(f"[DB-SEARCH] ETAPA 1: Buscando término exacto '{craving}'")
    exact_results = search_places_without_location(craving, limit)
    
    if exact_results:
        print(f"[DB-SEARCH] ✅ Encontrados {len(exact_results)} con término exacto")
        return exact_results, False
    
    # ETAPA 2: No encontró nada exacto, usar expansión de IA
    print(f"[DB-SEARCH] ETAPA 2: No encontró exacto, expandiendo con IA...")
    expanded_terms = await expand_search_terms_with_ai(craving, language, wa_id)
    
    # ✅ FASE 2: Obtener filtro de horarios del día
    today_filter = get_today_hours_filter()
    
    try:
        sql = f"""
        SELECT id, name, category, products, priority, cashback, hours, 
               address, phone, url_order, imagen_url, url_extra, afiliado,
               lat, lng, timezone, delivery,
               mon_open, mon_close, tue_open, tue_close, wed_open, wed_close,
               thu_open, thu_close, fri_open, fri_close, sat_open, sat_close,
               sun_open, sun_close
        FROM public.places 
        WHERE EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(categories) as item
            WHERE LOWER(item) LIKE ANY(%(search_patterns)s)
        )
        AND {today_filter}
        ORDER BY 
            (SELECT COUNT(*) FROM jsonb_array_elements_text(categories) as item
             WHERE LOWER(item) LIKE ANY(%(search_patterns)s)) DESC,
            CASE WHEN afiliado = true THEN 1 ELSE 0 END DESC,
            priority DESC,
            id ASC
        LIMIT %(limit)s;
        """
        
        # Crear patrones de búsqueda para todos los términos expandidos
        search_patterns = [f"%{term}%" for term in expanded_terms]
        
        params = {
            "search_patterns": search_patterns,
            "limit": limit
        }
        
        print(f"[DB-SEARCH] Buscando con expansión: {expanded_terms}")
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            
            results = []
            for row in rows:
                place = dict(row)
                place["products"] = list(place.get("products") or [])
                place["is_open_now"] = is_open_now_by_day(place)

                results.append(place)
            
            if results:
                print(f"[DB-SEARCH] ✅ Encontrados {len(results)} con expansión")
            else:
                print(f"[DB-SEARCH] ❌ No encontró nada ni con expansión")
            
            return results, True  # used_expansion=True
            
    except Exception as e:
        print(f"[DB-SEARCH] Error con expansión: {e}")
        return [], False
        return []

def search_places_with_location(craving: str, user_lat: float, user_lng: float, limit: int = 10) -> List[Dict[str, Any]]:
    """NUEVO ORDEN: producto -> afiliado -> prioridad -> distancia
    FASE 2: Solo muestra lugares con horarios del día actual"""
    if not craving:
        return []
    
    # ✅ FASE 2: Obtener filtro de horarios del día
    today_filter = get_today_hours_filter()
    
    try:
        sql = f"""
        WITH distances AS (
            SELECT id, name, category, products, priority, cashback, hours,
                   address, phone, url_order, imagen_url, url_extra, afiliado,
                   lat, lng, timezone, delivery,
                   mon_open, mon_close, tue_open, tue_close, wed_open, wed_close,
                   thu_open, thu_close, fri_open, fri_close, sat_open, sat_close,
                   sun_open, sun_close,
                   CASE 
                       WHEN lat IS NOT NULL AND lng IS NOT NULL THEN
                           6371000 * 2 * ASIN(SQRT(
                               POWER(SIN(RADIANS((lat - %(user_lat)s) / 2)), 2) +
                               COS(RADIANS(%(user_lat)s)) * COS(RADIANS(lat)) *
                               POWER(SIN(RADIANS((lng - %(user_lng)s) / 2)), 2)
                           ))
                       ELSE 999999
                   END as distance_meters,
                   (SELECT COUNT(*) FROM jsonb_array_elements_text(categories) as item
                    WHERE LOWER(item) LIKE ANY(%(search_patterns)s)) as product_match_score
            FROM public.places 
            WHERE EXISTS (
                SELECT 1 FROM jsonb_array_elements_text(categories) as item
                WHERE LOWER(item) LIKE ANY(%(search_patterns)s)
            )
            AND {today_filter}
        )
        SELECT * FROM distances
        ORDER BY 
            product_match_score DESC,
            CASE WHEN afiliado = true THEN 1 ELSE 0 END DESC,
            priority DESC,
            distance_meters ASC
        LIMIT %(limit)s;
        """
        
        # Crear patrones de búsqueda para el término original
        search_patterns = [f"%{craving.lower()}%"]
        
        params = {
            "search_patterns": search_patterns,
            "user_lat": user_lat,
            "user_lng": user_lng,
            "limit": limit
        }
        
        print(f"[DB-SEARCH] FASE 4 CON UBICACIÓN: Buscando '{craving}' en categories (SEO interno)")
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            
            results = []
            for row in rows:
                place = dict(row)
                place["products"] = list(place.get("products") or [])
                place["is_open_now"] = is_open_now_by_day(place)

                
                if place.get("distance_meters") and place["distance_meters"] < 999999:
                    place["distance_text"] = format_distance(place["distance_meters"])
                else:
                    place["distance_text"] = ""
                
                results.append(place)
            
            print(f"[DB-SEARCH] Con ubicación: {len(results)} resultados")
            return results
            
    except Exception as e:
        print(f"[DB-SEARCH] Error con ubicación: {e}")
        return []

async def search_places_with_location_ai(craving: str, user_lat: float, user_lng: float, language: str, wa_id: str, limit: int = 10) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Búsqueda con ubicación en DOS ETAPAS:
    1. Busca término exacto primero
    2. Si no encuentra nada, busca con expansión de IA
    
    Retorna: (resultados, used_expansion)
    """
    if not craving:
        return [], False
    
    # ETAPA 1: Buscar término EXACTO primero
    print(f"[DB-SEARCH] ETAPA 1 (con ubicación): Buscando término exacto '{craving}'")
    exact_results = search_places_with_location(craving, user_lat, user_lng, limit)
    
    if exact_results:
        print(f"[DB-SEARCH] ✅ Encontrados {len(exact_results)} con término exacto")
        return exact_results, False
    
    # ETAPA 2: No encontró nada exacto, usar expansión de IA
    print(f"[DB-SEARCH] ETAPA 2 (con ubicación): No encontró exacto, expandiendo con IA...")
    expanded_terms = await expand_search_terms_with_ai(craving, language, wa_id)
    
    # ✅ FASE 2: Obtener filtro de horarios del día
    today_filter = get_today_hours_filter()
    
    try:
        sql = f"""
        WITH distances AS (
            SELECT id, name, category, products, priority, cashback, hours,
                   address, phone, url_order, imagen_url, url_extra, afiliado,
                   lat, lng, timezone, delivery,
                   mon_open, mon_close, tue_open, tue_close, wed_open, wed_close,
                   thu_open, thu_close, fri_open, fri_close, sat_open, sat_close,
                   sun_open, sun_close,
                   CASE 
                       WHEN lat IS NOT NULL AND lng IS NOT NULL THEN
                           6371000 * 2 * ASIN(SQRT(
                               POWER(SIN(RADIANS((lat - %(user_lat)s) / 2)), 2) +
                               COS(RADIANS(%(user_lat)s)) * COS(RADIANS(lat)) *
                               POWER(SIN(RADIANS((lng - %(user_lng)s) / 2)), 2)
                           ))
                       ELSE 999999
                   END as distance_meters,
                   (SELECT COUNT(*) FROM jsonb_array_elements_text(categories) as item
                    WHERE LOWER(item) LIKE ANY(%(search_patterns)s)) as product_match_score
            FROM public.places 
            WHERE EXISTS (
                SELECT 1 FROM jsonb_array_elements_text(categories) as item
                WHERE LOWER(item) LIKE ANY(%(search_patterns)s)
            )
            AND {today_filter}
        )
        SELECT * FROM distances
        ORDER BY 
            product_match_score DESC,
            CASE WHEN afiliado = true THEN 1 ELSE 0 END DESC,
            priority DESC,
            distance_meters ASC
        LIMIT %(limit)s;
        """
        
        # Crear patrones de búsqueda para todos los términos expandidos
        search_patterns = [f"%{term}%" for term in expanded_terms]
        
        params = {
            "search_patterns": search_patterns,
            "user_lat": user_lat,
            "user_lng": user_lng,
            "limit": limit
        }
        
        print(f"[DB-SEARCH] Buscando con expansión y ubicación: {expanded_terms}")
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            
            results = []
            for row in rows:
                place = dict(row)
                place["products"] = list(place.get("products") or [])
                place["is_open_now"] = is_open_now_by_day(place)

                
                if place.get("distance_meters") and place["distance_meters"] < 999999:
                    place["distance_text"] = format_distance(place["distance_meters"])
                else:
                    place["distance_text"] = ""
                
                results.append(place)
            
            if results:
                print(f"[DB-SEARCH] ✅ Encontrados {len(results)} con expansión y ubicación")
            else:
                print(f"[DB-SEARCH] ❌ No encontró nada ni con expansión")
            
            return results, True  # used_expansion=True
            
    except Exception as e:
        print(f"[DB-SEARCH] Error con expansión y ubicación: {e}")
        return [], False

def format_results_list(results: List[Dict[str, Any]], language: str) -> str:
    """Lista estilizada con información completa del negocio incluyendo horarios. SIEMPRE EN ESPAÑOL."""
    if not results:
        return ""

    lines: List[str] = []

    for idx, place in enumerate(results, 1):
        name = place.get("name") or place.get("name_es") or place.get("name_en") or "Sin nombre"
        distance = place.get("distance_text", "") or ""
        # ✅ FIX: Priorizar url_extra (columna X) sobre url_order
        url = place.get("url_extra") or place.get("url_order") or ""
        cashback = bool(place.get("cashback", False))

        # Servicio a domicilio
        has_delivery = bool(place.get("delivery"))

        # ✅ NUEVO: Usar columnas individuales de horarios
        is_open, hours_info, has_hours = get_hours_status_from_columns(place)

        # ✅ FASE 2: Determinar el título basado en el estado de horarios
        # Ya no hay caso "HORARIO NO DISPONIBLE" porque filtramos en SQL
        if is_open:
            title = f"📍 {idx}) {name} 🟢 ABIERTO"
            if hours_info:
                title += f" ({hours_info})"
        else:
            title = f"📍 {idx}) {name} 🔴 CERRADO"
            if hours_info:
                title += f" ({hours_info})"

        block = [title]
        
        # ✅ FASE 1 - CAMBIO 2: Solo mostrar delivery si tiene
        if has_delivery:
            block.append(f"🛵 Servicio a domicilio ✅")
        
        block.append(f"💳 Acumula cashback: {'Sí 💰' if cashback else 'No'}")

        if distance:
            block.append(f"📍 Distancia: {distance}")

        if url:
            block.append(f"🔗 Ver el lugar: {url}")

        lines.append("\n".join(block))

    return "\n\n".join(lines)


def format_place_details(place: Dict[str, Any], language: str) -> str:
    """Detalles completos de un lugar con cashback y horarios. SIEMPRE EN ESPAÑOL."""
    name = place.get("name", "Sin nombre")
    address = place.get("address", "Dirección no disponible")
    phone = place.get("phone", "")
    url_order = place.get("url_order", "")
    url_extra = place.get("url_extra", "")
    distance = place.get("distance_text", "")
    products = place.get("products", [])
    cashback = place.get("cashback", False)
    hours = place.get("hours", {})
    delivery = place.get("delivery", False)  # ✅ NUEVO: Obtener delivery
    
    # ✅ FIX: Priorizar url_extra (columna X) sobre url_order
    main_url = url_extra or url_order
    is_open, hours_info = is_place_open(hours)
    
    lines = [f"📍 {name}"]
    
    # Estado de apertura
    if is_open:
        lines.append(f"🟢 ABIERTO {f'({hours_info})' if hours_info else ''}")
    else:
        lines.append(f"🔴 CERRADO {f'({hours_info})' if hours_info else ''}")
    
    # Cashback destacado
    if cashback:
        lines.append("💰 ¡CON CASHBACK DISPONIBLE! 🎉")
    
    if distance:
        lines.append(f"🚗 A {distance} de ti")
    
    lines.append(f"📍 {address}")
    
    if phone:
        lines.append(f"📞 {phone}")
    
    if main_url:
        lines.append(f"🔗 {main_url}")
    
    # ✅ NUEVO: Mostrar link de delivery si está disponible
    if delivery and url_order:
        lines.append(f"🚚 Pedir a domicilio: {url_order}")
    
    # Mostrar horarios de la semana si existen
    if hours:
        lines.append("\n⏰ Horarios:")
        day_names = {
            'mon': 'Lun', 'tue': 'Mar', 'wed': 'Mié',
            'thu': 'Jue', 'fri': 'Vie', 'sat': 'Sáb', 'sun': 'Dom'
        }
        for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']:
            if day in hours:
                schedules = hours[day]
                times = []
                for schedule in schedules:
                    if len(schedule) == 2:
                        times.append(f"{schedule[0]}-{schedule[1]}")
                if times:
                    lines.append(f"  {day_names[day]}: {', '.join(times)}")
    
    if products and len(products) > 0:
        products_text = ", ".join(products[:6])
        lines.append(f"\n🍽️ Especialidades: {products_text}")
    
    lines.append("\n¿Te interesa otro lugar o quieres que busque algo más? 😊")
    
    return "\n".join(lines)

def get_smart_response_message(results: List[Dict[str, Any]], craving: str, language: str, has_location: bool) -> str:
    """Respuestas humanas según cantidad. SIEMPRE EN ESPAÑOL."""
    count = len(results)
    
    if count == 0:
        return f"No encontré lugares que tengan {craving}" + (" cerca de ti" if has_location else "") + " 😕"
    elif count == 1:
        place_name = results[0].get("name", "un lugar")
        return f"Solo conozco un lugar donde tienen {craving}: {place_name}" + (" cerca de ti" if has_location else "")
    elif count <= 3:
        return f"Te conseguí {count} lugares que tienen {craving}" + (" cerca de ti:" if has_location else ":")
    else:
        return f"Mira, te conseguí {count} opciones de {craving}" + (" cerca de ti:" if has_location else ":")

# ================= SALUDOS CON IA =================
async def generate_humanized_greeting(name: str, language: str) -> str:
    """Genera saludo humanizado con IA. SIEMPRE EN ESPAÑOL."""
    if not OPENAI_API_KEY:
        return get_fallback_greeting(name, language)
    
    try:
        system_prompt = f"""Eres {name}, una persona real que trabaja recomendando lugares para comer en México.
Escribes como un amigo cálido por WhatsApp - natural, casual, sin ser formal.
NUNCA menciones que eres un bot, IA o asistente automático.
Usa 1-2 emojis máximo. Preséntate brevemente y pregunta qué se le antoja.
Máximo 3 líneas. No uses markdown."""
        
        user_prompt = "Genera tu primer mensaje de saludo para alguien que acaba de escribirte."
        
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "gpt-4o-mini",
                    "temperature": 0.8,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "max_tokens": 120
                }
            )
        
        if response.status_code == 200:
            data = response.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            
            if content and len(content) > 10:
                content = re.sub(r'\*\*([^*]+)\*\*', r'\1', content)
                content = re.sub(r'\*([^*]+)\*', r'\1', content)
                content = content.replace('```', '').replace('`', '')
                
                lines = [line.strip() for line in content.split('\n') if line.strip()]
                if len(lines) > 3:
                    content = '\n'.join(lines[:3])
                
                print(f"[GREETING] IA generó saludo para {name} (es)")
                return content
        
        return get_fallback_greeting(name, language)
        
    except Exception as e:
        print(f"[GREETING] Error con IA: {e}")
        return get_fallback_greeting(name, language)

def detect_non_spanish_greeting(text: str) -> bool:
    """
    Detecta si el mensaje es un saludo en otro idioma (NO español).
    USA LISTA BLANCA: Solo permite saludos específicos en español.
    TODO lo demás se considera otro idioma.
    Retorna True si detecta otro idioma, False si es español válido.
    """
    text_lower = text.lower().strip()
    
    # ✅ LISTA BLANCA: Saludos PERMITIDOS en español
    spanish_greetings = [
        'hola', 'buenos dias', 'buenas dias', 'buen dia', 'buenas tardes', 'buenas noches',
        'buen día', 'buenas días',  # Con acento
        'que tal', 'qué tal', 'que onda', 'qué onda',
        'saludos', 'holi', 'holaaa', 'holaa'
    ]
    
    # Verificar si el mensaje O las primeras palabras coinciden con español
    words = text_lower.split()
    
    # Verificar mensaje completo
    if text_lower in spanish_greetings:
        return False  # Es español válido
    
    # Verificar si empieza con saludo español
    for greeting in spanish_greetings:
        if text_lower.startswith(greeting + ' ') or text_lower.startswith(greeting + ',') or text_lower.startswith(greeting + '!'):
            return False  # Es español válido
        # Verificar primeras dos palabras (para "buenos dias", etc.)
        if len(words) >= 2:
            two_words = ' '.join(words[:2])
            if two_words == greeting:
                return False  # Es español válido
    
    # ✅ Si llegamos aquí y parece ser un saludo (corto, sin caracteres especiales), es otro idioma
    # Detectar si es un saludo (mensajes muy cortos de 1-3 palabras sin caracteres especiales)
    if len(words) <= 3 and len(text) <= 30:
        # Es un mensaje corto que NO está en la lista blanca de español
        # Muy probablemente es un saludo en otro idioma
        return True
    
    # Si es un mensaje más largo y no es un saludo español, no asumimos que es saludo en otro idioma
    return False

def get_fallback_greeting(name: str, language: str) -> str:
    """Fallback de saludo. SIEMPRE EN ESPAÑOL."""
    templates = [
        f"¡Hola! Soy {name} 😊 ¿Qué antojo tienes hoy?",
        f"¡Hey! Me llamo {name} 🍽️ ¿Se te antoja algo en particular?",
        f"¡Hola! Soy {name} ¿Qué tienes ganas de comer? 😋"
    ]
    return random.choice(templates)

# ================= ROUTES =================
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "time": local_now().isoformat(),
        "dry_run": not SEND_VIA_WHATSAPP,
        "active_sessions": len(user_sessions),
        "db_connected": True
    }
@app.get("/debug/cashback")
async def debug_cashback_database():
    """Debug endpoint para verificar valores de cashback en la BD"""
    try:
        sql = """
        SELECT id, name, cashback, afiliado, products, delivery
        FROM public.places 
        WHERE products::text ILIKE '%jugo%'
        ORDER BY name
        LIMIT 10;
        """
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            
            results = []
            for row in rows:
                place = dict(row)
                place["products"] = list(place.get("products") or [])
                results.append({
                    "id": place["id"],
                    "name": place["name"],
                    "cashback": place["cashback"],
                    "cashback_type": type(place["cashback"]).__name__,
                    "afiliado": place["afiliado"],
                    "has_jugo": any("jugo" in p.lower() for p in place["products"])
                })
            
            return {
                "status": "ok",
                "count": len(results),
                "places_with_jugo": results
            }
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/debug/verify/{place_name}")
async def debug_verify_place(place_name: str):
    """Verificar un lugar específico por nombre"""
    try:
        sql = """
        SELECT id, name, cashback, afiliado, products, delivery
        FROM public.places 
        WHERE LOWER(name) LIKE %s
        ORDER BY name
        LIMIT 5;
        """
        
        search_pattern = f"%{place_name.lower()}%"
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, (search_pattern,))
            rows = cur.fetchall()
            
            if not rows:
                return {"status": "not_found", "search": place_name}
            
            results = []
            for row in rows:
                place = dict(row)
                place["products"] = list(place.get("products") or [])
                results.append({
                    "id": place["id"],
                    "name": place["name"],
                    "cashback": place["cashback"],
                    "afiliado": place["afiliado"],
                    "products_sample": place["products"][:3] if place["products"] else []
                })
            
            return {
                "status": "ok",
                "count": len(results),
                "places": results
            }
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/debug/hours/{place_id}")
async def debug_place_hours(place_id: int):
    """Ver los horarios de un lugar específico"""
    try:
        sql = """
        SELECT id, name, hours, cashback
        FROM public.places 
        WHERE id = %s;
        """
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, (place_id,))
            row = cur.fetchone()
            
            if row:
                place = dict(row)
                # Convertir jsonb a dict si es necesario
                hours = dict(place.get("hours", {})) if place.get("hours") else {}
                
                return {
                    "id": place["id"],
                    "name": place["name"],
                    "hours": hours,
                    "hours_type": type(hours).__name__,
                    "cashback": place["cashback"]
                }
            else:
                return {"status": "not_found", "id": place_id}
                
    except Exception as e:
        return {"status": "error", "message": str(e)}

# 3. AGREGAR comando especial de reset de cashback
@app.post("/debug/fix-cashback/{negocio_id}")
async def fix_cashback_real(negocio_id: int, value: bool):
    """Corregir cashback en la tabla real"""
    try:
        sql = """
        INSERT INTO negocios_bot_meta (negocio_id, cashback)
        VALUES (%s, %s)
        ON CONFLICT (negocio_id) 
        DO UPDATE SET cashback = %s
        RETURNING negocio_id, cashback;
        """
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, (negocio_id, value, value))
            row = cur.fetchone()
            
            if row:
                return {
                    "status": "updated",
                    "negocio_id": row["negocio_id"],
                    "cashback": row["cashback"]
                }
            else:
                return {"status": "failed"}
                
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/debug/tables")
async def debug_database_structure():
    """Ver la estructura real de la base de datos"""
    try:
        sql = """
        SELECT table_name, table_type 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name;
        """
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            
            tables = []
            views = []
            for row in rows:
                if row["table_type"] == "BASE TABLE":
                    tables.append(row["table_name"])
                else:
                    views.append(row["table_name"])
            
            # Buscar la definición de la vista places
            cur.execute("SELECT definition FROM pg_views WHERE viewname = 'places'")
            view_def = cur.fetchone()
            
            return {
                "tables": tables,
                "views": views,
                "places_view_definition": view_def["definition"] if view_def else None
            }
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/webhook")
async def verify_webhook(request: Request):
    query = request.query_params
    mode = query.get("hub.mode")
    token = query.get("hub.verify_token") 
    challenge = query.get("hub.challenge")
    
    if mode == "subscribe" and token == VERIFY_TOKEN:
        print("[WEBHOOK] Verificación exitosa")
        return PlainTextResponse(challenge or "")
    
    print(f"[WEBHOOK] Verificación fallida: mode={mode}, token={token}")
    raise HTTPException(status_code=403, detail="Verificación fallida")

@app.post("/webhook")
async def handle_webhook(request: Request):
    body = await request.body()
    
    if not verify_signature(request, body):
        print("[WEBHOOK] Firma inválida")
        raise HTTPException(status_code=403, detail="Firma inválida")
    
    try:
        data = await request.json()
    except Exception as e:
        print(f"[WEBHOOK] JSON inválido: {e}")
        raise HTTPException(status_code=400, detail="JSON inválido")
    
    entries = data.get("entry", [])
    if not entries:
        return {"status": "no entries"}
    
    changes = entries[0].get("changes", [])
    if not changes:
        return {"status": "no changes"}
    
    value = changes[0].get("value", {})
    
    # ✅ NUEVO: Extraer phone_number_id
    phone_number_id = value.get("metadata", {}).get("phone_number_id", "")
    config = get_environment_config(phone_number_id)
    
    messages = value.get("messages", [])
    
    if not messages:
        return {"status": "no messages"}
    
    message = messages[0]
    from_wa = message.get("from", "")
    message_type = message.get("type", "")
    
    print(f"{config['prefix']} [WEBHOOK] Mensaje de {from_wa}, tipo: {message_type}")  # ✅ Agregado prefix
    
    if message_type == "text":
        text = message.get("text", {}).get("body", "").strip()
        await handle_text_message(from_wa, text, phone_number_id)  # ✅ Agregado phone_number_id
        
    elif message_type == "location":
        location = message.get("location", {})
        lat = location.get("latitude")
        lng = location.get("longitude") 
        if lat and lng:
            await handle_location_message(from_wa, float(lat), float(lng), phone_number_id)  # ✅ Agregado
        
    else:
        print(f"{config['prefix']} [WEBHOOK] Tipo de mensaje no soportado: {message_type}")  # ✅ Agregado prefix
    
    return {"status": "processed"}


@app.get("/debug/test-hours/{place_id}")
async def test_place_hours(place_id: int):
    """
    Endpoint para probar la lógica de horarios con un lugar específico
    Uso: GET http://localhost:8000/debug/test-hours/123
    """
    try:
        sql = """
        SELECT id, name, hours
        FROM public.places 
        WHERE id = %s;
        """
        
        with get_pool().connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, (place_id,))
            row = cur.fetchone()
            
            if not row:
                return {"status": "not_found", "id": place_id}
            
            place = dict(row)
            hours = dict(place.get("hours", {})) if place.get("hours") else {}
            
            # Probar la función is_place_open
            is_open, next_hours = is_place_open(hours)
            
            now = local_now()
            current_day = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'][now.weekday()]
            current_time = now.strftime('%H:%M')
            
            return {
                "id": place["id"],
                "name": place["name"],
                "current_day": current_day,
                "current_time": current_time,
                "hours_json": hours,
                "is_open": is_open,
                "next_hours": next_hours,
                "status": "✅ ABIERTO" if is_open else "❌ CERRADO"
            }
            
    except Exception as e:
        import traceback
        return {
            "status": "error", 
            "message": str(e),
            "traceback": traceback.format_exc()
        }

async def handle_text_message(wa_id: str, text: str, phone_number_id: str = None):
    config = get_environment_config(phone_number_id) if phone_number_id else {"prefix": ""}
    print(f"{config.get('prefix', '')} [TEXT] {wa_id}: {text}")
    
    # ✅ PRIMERO: Detectar saludos comunes en inglés y otros idiomas
    text_lower = text.lower().strip()
    non_spanish_greetings = [
        # Inglés
        'hi', 'hii', 'hiii', 'hello', 'helo', 'hey', 'heya', 
        'good morning', 'good afternoon', 'good evening', 'good night',
        'greetings', 'howdy', 'hiya', 'sup', 'yo',
        # Francés
        'bonjour', 'salut', 'bonsoir', 'coucou',
        # Italiano
        'ciao', 'buongiorno', 'buonasera', 'salve',
        # Alemán
        'hallo', 'guten tag', 'guten morgen', 'guten abend',
        # Portugués
        'oi', 'olá', 'ola', 'bom dia', 'boa tarde', 'boa noite',
        # Otros
        'namaste', 'shalom', 'aloha', 'konnichiwa'
    ]
    
    # Verificar si es un saludo en otro idioma
    is_non_spanish_greeting = (
        text_lower in non_spanish_greetings or
        any(text_lower.startswith(greeting + ' ') or text_lower.startswith(greeting + ',') 
            for greeting in non_spanish_greetings)
    )
    
    if is_non_spanish_greeting:
        print(f"[LANG-DETECT] Saludo en otro idioma detectado: '{text}'")
        spanish_invitation = (
            "Hi! 👋 Please write in Spanish so I can help you better. Thanks! 😊\n\n"
            "Hola! 👋 Por favor escribe en español para poder ayudarte mejor. ¡Gracias! 😊"
        )
        await send_whatsapp_message(wa_id, spanish_invitation, phone_number_id)
        return
    
    # ✅ SEGUNDO: Detectar si el mensaje NO está en español
    if not is_spanish(text):
        print(f"[LANG-DETECT] Mensaje no está en español, invitando a escribir en español")
        spanish_invitation = (
            "Hi! 👋 Please write in Spanish so I can help you better. Thanks! 😊\n\n"
            "Hola! 👋 Por favor escribe en español para poder ayudarte mejor. ¡Gracias! 😊"
        )
        await send_whatsapp_message(wa_id, spanish_invitation, phone_number_id)
        return
    
    # ✅ Si está en español, continuar normalmente (siempre con idioma "es")
    session = get_or_create_user_session(wa_id)
    
    intent_data = await extract_intent_with_ai(text, session["language"], session["name"], wa_id)
    intent = intent_data.get("intent", "other")
    craving = intent_data.get("craving")
    needs_location = intent_data.get("needs_location", False)
    business_name = intent_data.get("business_name")  # ✅ NUEVO
    
    time_since_last = time.time() - session.get("last_seen", 0)
    is_new_session = session.get("is_new")
    has_greeting_words = any(word in text.lower() for word in ['hola', 'hello', 'hi', 'buenas', 'buenos'])
    
    # ✅ NUEVO: BÚSQUEDA POR NOMBRE DE NEGOCIO
    if intent == "business_search" and business_name:
        session["is_new"] = False
        
        place = search_place_by_name(business_name)
        
        if place:
            # Enviar detalles del negocio directamente
            details = format_place_details(place, session["language"])
            await send_whatsapp_message(wa_id, details, phone_number_id)
            
            # Enviar imagen si existe
            image_url = place.get("imagen_url")
            if image_url:
                await send_whatsapp_image(wa_id, image_url, phone_number_id=phone_number_id)
            
            # Guardar en sesión por si quiere más info
            session["last_results"] = [place]
        else:
            # No encontrado - SIEMPRE EN ESPAÑOL
            response = f"No encontré '{business_name}' en mi lista 😕 ¿Quieres que busque algo más o me dices qué tipo de comida te gustaría?"
            await send_whatsapp_message(wa_id, response, phone_number_id)
        
        return
    
    # ESCENARIO 1: Solo saludo sin craving
    if ((is_new_session and not craving) or 
        (intent == "greeting" and not craving and time_since_last > IDLE_RESET_SECONDS)):
        
        # ✅ NUEVO: Detectar si el saludo es en inglés/otro idioma
        if detect_non_spanish_greeting(text):
            response = (
                "Hi! 👋 Please write in Spanish so I can help you better. Thanks! 😊\n\n"
                "Hola! 👋 Por favor escribe en español para poder ayudarte mejor. ¡Gracias! 😊"
            )
            await send_whatsapp_message(wa_id, response, phone_number_id)
            session["is_new"] = False
            return
        
        # Saludo en español - continuar normal
        greeting = await generate_humanized_greeting(session["name"], session["language"])
        await send_whatsapp_message(wa_id, greeting, phone_number_id)
        session["is_new"] = False
        return
    
    # PASO 3: SELECCIÓN POR NÚMERO (1-5 o más)
    if re.match(r'^\s*\d+\s*$', text) and session.get("last_results"):
        try:
            selected_number = int(text.strip())
            results = session.get("last_results", [])

            if 1 <= selected_number <= len(results):
                selected_place = results[selected_number - 1]
                details = format_place_details(selected_place, session["language"])
                await send_whatsapp_message(wa_id, details, phone_number_id)

                image_url = selected_place.get("imagen_url")
                if image_url:
                    await send_whatsapp_image(wa_id, image_url, phone_number_id=phone_number_id)

                return
            else:
                response = f"Elige un número del 1 al {len(results)}, porfa 😊"
                await send_whatsapp_message(wa_id, response)
                return
        except ValueError:
            pass

    
    # ESCENARIOS 2 y 3: Hay craving con saludo
    if craving and (is_new_session or (has_greeting_words and craving)):
        session["is_new"] = False
        session["last_search"] = {"craving": craving, "needs_location": needs_location}
        
        if session.get("user_location"):
            user_lat = session["user_location"]["lat"]
            user_lng = session["user_location"]["lng"] 
            results, used_expansion = await search_places_with_location_ai(craving, user_lat, user_lng, session["language"], wa_id, 10)
        else:
            results, used_expansion = await search_places_without_location_ai(craving, session["language"], wa_id, 10)
        
        # Limitar a 5 para mostrar, pero buscar hasta 10
        display_results = results[:MAX_SUGGESTIONS]
        print(f"[DEBUG] FINAL: {len(display_results)} resultados enviados de {len(results)} encontrados")
        
        if display_results:
            session["last_results"] = display_results  # Guardamos todos los resultados
            
            # ✅ NUEVO: Si usó expansión, avisar al usuario
            if used_expansion:
                intro_message = f"No encontré {craving} exactamente, pero estos lugares tienen platillos similares"
            else:
                intro_message = get_smart_response_message(display_results, craving, session["language"], session.get("user_location") is not None)
            
            results_list = format_results_list(display_results, session["language"])
            
            # ✅ SIEMPRE mostrar la lista, incluso si hay solo 1 resultado
            response = f"¡Hola! {intro_message}\n\n{results_list}\n\nEscribe el número del que te llame la atención"
            if not session.get("user_location"):
                response += " o pásame tu ubicación para ver qué hay por tu zona 📍"
            
            await send_whatsapp_message(wa_id, response)
        else:
            response = f"¡Hola! Ay no, no tengo {craving} en mi lista. ¿Qué tal si me dices otra cosa que se te antoje o me mandas tu ubicación para ver qué opciones hay por ahí?"
            await send_whatsapp_message(wa_id, response)
        return
    
    # BÚSQUEDAS REGULARES: Solo craving sin saludo en sesión existente
    if intent == "search" and craving and not is_new_session:
        session["last_search"] = {"craving": craving, "needs_location": needs_location}
        
        if session.get("user_location"):
            user_lat = session["user_location"]["lat"]
            user_lng = session["user_location"]["lng"] 
            results, used_expansion = await search_places_with_location_ai(craving, user_lat, user_lng, session["language"], wa_id, 10)
        else:
            results, used_expansion = await search_places_without_location_ai(craving, session["language"], wa_id, 10)
        
        # Limitar a 5 para mostrar, pero buscar hasta 10
        display_results = results[:MAX_SUGGESTIONS]
        print(f"[DEBUG REGULAR] FINAL: {len(display_results)} resultados enviados de {len(results)} encontrados")
        
        if display_results:
            session["last_results"] = display_results  # Guardamos todos los resultados
            
            # ✅ NUEVO: Si usó expansión, avisar al usuario
            if used_expansion:
                intro_message = f"No encontré {craving} exactamente, pero estos lugares tienen platillos similares"
            else:
                intro_message = get_smart_response_message(display_results, craving, session["language"], session.get("user_location") is not None)
            
            results_list = format_results_list(display_results, session["language"])
            
            # ✅ SIEMPRE mostrar la lista, incluso si hay solo 1 resultado
            response = f"{intro_message}\n\n{results_list}\n\nMándame el número del que te guste"
            
            if not session.get("user_location"):
                response += " o mándame tu ubicación para ver qué hay cerca 📍"
            
            await send_whatsapp_message(wa_id, response)
        else:
            if session.get("user_location"):
                response = f"Ay no, no encontré {craving} cerca de ti 😕 ¿Tienes ganas de algo más?"
            else:
                response = f"No tengo {craving} en mi lista. ¿Qué tal otra cosa o me mandas tu ubicación?"
            
            await send_whatsapp_message(wa_id, response)
        return
    
    # OTROS CASOS
    elif intent == "other":
        response = "Oye, cuéntame qué se te antoja comer y te ayudo a encontrar algo bueno por ahí 😊"
        await send_whatsapp_message(wa_id, response)
    
    else:
        response = "¿En qué te puedo echar la mano? Dime qué comida tienes ganas de probar 🍽️"
        await send_whatsapp_message(wa_id, response)



        

async def handle_location_message(wa_id: str, lat: float, lng: float, phone_number_id: str = None):
    config = get_environment_config(phone_number_id) if phone_number_id else {"prefix": ""}
    print(f"{config.get('prefix', '')} [LOCATION] {wa_id}: lat={lat}, lng={lng}")
    
    if wa_id not in user_sessions:
        print(f"[LOCATION] No hay sesión para {wa_id}")
        return
    
    session = user_sessions[wa_id]
    session["user_location"] = {"lat": lat, "lng": lng}
    session["last_seen"] = time.time()
    
    if session.get("last_search") and session["last_search"].get("craving"):
        craving = session["last_search"]["craving"]
        results, used_expansion = await search_places_with_location_ai(craving, lat, lng, session["language"], wa_id, 10)

        # Limitar a MAX_SUGGESTIONS para mostrar, pero buscar hasta 10
        display_results = results[:MAX_SUGGESTIONS]
        print(f"[DEBUG UBICACIÓN] FINAL: {len(display_results)} resultados enviados de {len(results)} encontrados")

        if display_results:
            session["last_results"] = display_results
            
            # ✅ NUEVO: Si usó expansión, avisar al usuario
            if used_expansion:
                intro_message = f"No encontré {craving} exactamente, pero estos lugares tienen platillos similares cerca de ti"
            else:
                intro_message = get_smart_response_message(display_results, craving, session["language"], True)
            
            results_list = format_results_list(display_results, session["language"])

            # ✅ SIEMPRE mostrar la lista, incluso si hay solo 1 resultado
            response = f"{intro_message}\n\n{results_list}\n\nMándame el número del que te guste 📍"

            await send_whatsapp_message(wa_id, response, phone_number_id)
        else:
            response = f"No encontré {craving} cerca de ti 😕 ¿Qué tal si probamos con otra cosa?"
            await send_whatsapp_message(wa_id, response, phone_number_id)

        return



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)



# =============== SHEET SYNC (non-breaking, with hours) ===============
from fastapi import Body
import json, re

SHEET_SYNC_SECRET = os.getenv("SHEET_SYNC_SECRET", "")

# columnas aceptadas desde el Sheet (agregamos horarios *_open/_close para cada día)
_SHEET_ALLOWED = set([
    "id","name_es","name_en","category","tags_es","tags_en","address",
    "neighborhood","city","state","country","postal_code","lat","lon",
    "timezone","priority","is_active","affiliate","cashback",
    "cover_image_url","logo_url","gallery_urls"
])

_DAYS = ["mon","tue","wed","thu","fri","sat","sun"]

# ✅ NUEVO: Aceptar AMBOS formatos de horarios
# Formato 1: mon_open, mon_close (Google Sheets actual)
# Formato 2: mon_1_open, mon_1_close, mon_2_open, mon_2_close (futuro)
for d in _DAYS:
    # Formato simple (compatibilidad con Sheets actuales)
    _SHEET_ALLOWED.add(f"{d}_open")
    _SHEET_ALLOWED.add(f"{d}_close")
    
    # Formato con intervalos (para múltiples horarios por día)
    _SHEET_ALLOWED.update([f"{d}_1_open", f"{d}_1_close", f"{d}_2_open", f"{d}_2_close"])

# === helpers de tipos ===
def _ss_to_bool(v):
    """Convierte valores de Google Sheets a booleano correctamente"""
    if v is None: 
        return None
    
    # Si ya es booleano, retornarlo directamente
    if isinstance(v, bool): 
        return v
    
    # Convertir a string y normalizar
    s = str(v).strip().lower()
    
    # IMPORTANTE: Manejar strings "true"/"false" que envía Google Sheets
    if s in {"true", "1", "si", "sí", "yes", "y", "verdadero", "t"}:
        return True
    if s in {"false", "0", "no", "n", "falso", "f", ""}:
        return False
    
    # Log para debug
    print(f"[DEBUG-BOOL] Valor no reconocido: '{v}' (tipo: {type(v)})")
    return None

def _ss_to_float(v):
    if v in (None, ""): return None
    try: return float(v)
    except: return None

def _ss_to_int(v):
    if v in (None, ""): return None
    try: return int(float(v))
    except: return None

def _ss_split_list(v):
    if v is None: return None
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s: return None
    return [p.strip() for p in s.split(",") if p.strip()]

# === validación mínima ===
def _ss_validate_min(row):
    if not row.get("id"):
        raise HTTPException(status_code=422, detail="Falta 'id'")
    if not (row.get("name_es") or row.get("name_en")):
        raise HTTPException(status_code=422, detail="Falta 'name_es' o 'name_en'")
    addr = row.get("address")
    lat, lon = row.get("lat"), row.get("lon")
    if not (addr or (lat is not None and lon is not None)):
        raise HTTPException(status_code=422, detail="Falta 'address' o (lat y lon)")

# === horarios: HH:mm -> estructura JSON por día (ACTUALIZADO) ===

def _norm_time(s):
    """
    Normaliza tiempo desde Google Sheet a formato HH:MM
    Maneja: "8:30:00", "08:30:00", "8:30", "08:30", "8:00", etc.
    """
    if s is None: 
        return None
    
    s = str(s).strip()
    if not s: 
        return None
    
    # ✅ FIX: Remover segundos si existen: "8:30:00" -> "8:30"
    if s.count(':') == 2:
        parts = s.split(':')
        s = f"{parts[0]}:{parts[1]}"  # Quitar segundos
    
    # Separar horas y minutos
    parts = s.split(":")
    if len(parts) != 2:
        return None
    
    try:
        hh = int(parts[0])
        mm = int(parts[1])
        
        # Validar rangos
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return None
        
        # ✅ Retornar siempre con zero-padding: "8:30" -> "08:30"
        return f"{hh:02d}:{mm:02d}"
    
    except (ValueError, IndexError):
        return None

def _normalize_hours_from_sheet(row):
    """
    Normaliza horarios desde Google Sheets a formato de columnas individuales de PostgreSQL.
    
    Maneja DOS formatos:
    1. SIMPLE: mon_open, mon_close (actual en Google Sheets)
    2. INTERVALOS: mon_1_open, mon_1_close, mon_2_open, mon_2_close (futuro)
    
    Returns:
        dict con claves: mon_open, mon_close, tue_open, ..., sun_close
    """
    result = {}
    
    for d in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']:
        open_key = f"{d}_open"
        close_key = f"{d}_close"
        
        # PRIORIDAD 1: Formato simple (mon_open, mon_close)
        simple_open = row.get(open_key)
        simple_close = row.get(close_key)
        
        # PRIORIDAD 2: Formato con intervalos (mon_1_open, mon_1_close)
        interval_open = row.get(f"{d}_1_open")
        interval_close = row.get(f"{d}_1_close")
        
        # Usar el que esté disponible (prioridad al formato simple)
        final_open = simple_open if simple_open is not None else interval_open
        final_close = simple_close if simple_close is not None else interval_close
        
        # Solo agregar si ambos existen
        if final_open is not None and final_close is not None:
            result[open_key] = _norm_time(final_open)
            result[close_key] = _norm_time(final_close)
            
            if result[open_key] and result[close_key]:
                print(f"[SHEET-NORMALIZE] {open_key}={result[open_key]}, {close_key}={result[close_key]}")
    
    return result

def _extract_hours(row):
    """
    Lee horarios desde Google Sheet y los normaliza correctamente.
    Genera: {"mon":[["08:00","20:00"],["21:00","23:00"]], ...}
    
    MANEJA:
    - Formatos con segundos: "8:30:00" -> "08:30"
    - Sin zero-padding: "8:30" -> "08:30"
    - Horarios que cruzan medianoche: "22:00" - "02:00"
    """
    hours = {}
    
    for d in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']:
        day_list = []
        
        for i in (1, 2):  # Dos posibles intervalos por día
            open_raw = row.get(f"{d}_{i}_open")
            close_raw = row.get(f"{d}_{i}_close")
            
            # ✅ Normalizar tiempos (maneja formato con segundos)
            o = _norm_time(open_raw)
            c = _norm_time(close_raw)
            
            # Validar que ambos existan
            if o and c:
                # ✅ PERMITIR horarios que cruzan medianoche
                # No validar o < c porque puede ser 22:00 - 02:00
                day_list.append([o, c])
                print(f"[SHEET-HOURS] {d}_{i}: {open_raw} -> {o} | {close_raw} -> {c}")
            elif o or c:
                # Si solo uno existe, loguear advertencia
                print(f"[SHEET-HOURS] ⚠️ {d}_{i}: Incompleto - open={open_raw}, close={close_raw}")
        
        if day_list:
            hours[d] = day_list
    
    return hours if hours else None

# === mapeo Sheet -> public.places ===
def _ss_map_to_places(row):
    # name preferencia: es -> en
    name = row.get("name_es") or row.get("name_en")

    # productos desde tags_es/tags_en
    products_es = _ss_split_list(row.get("tags_es"))
    products_en = _ss_split_list(row.get("tags_en"))
    products = (products_es or []) + (products_en or [])
    products = [p.lower() for p in products] if products else None

    # ✅ NUEVO: Horarios en AMBOS formatos
    # 1. JSON hours (para compatibilidad con código viejo)
    hours = _extract_hours(row)
    
    # 2. Columnas individuales (mon_open, tue_open, etc.) - USADO POR EL BOT
    normalized_hours = _normalize_hours_from_sheet(row)

    # Procesar valores booleanos con debug mejorado
    cashback_raw = row.get("cashback")
    cashback_bool = _ss_to_bool(cashback_raw)
    
    affiliate_raw = row.get("affiliate")
    affiliate_bool = _ss_to_bool(affiliate_raw)
    
    # Debug logging más detallado
    print(f"[DEBUG-MAPPING] ========== SYNC DEBUG ==========")
    print(f"[DEBUG-MAPPING] ID: {row.get('id')} - Name: {name}")
    print(f"[DEBUG-MAPPING] cashback RAW: '{cashback_raw}' (tipo: {type(cashback_raw).__name__})")
    print(f"[DEBUG-MAPPING] cashback BOOL: {cashback_bool} (tipo: {type(cashback_bool).__name__})")
    print(f"[DEBUG-MAPPING] affiliate RAW: '{affiliate_raw}' (tipo: {type(affiliate_raw).__name__})")
    print(f"[DEBUG-MAPPING] affiliate BOOL: {affiliate_bool} (tipo: {type(affiliate_bool).__name__})")
    print(f"[DEBUG-MAPPING] Products: {products[:3] if products else 'None'}...")
    print(f"[DEBUG-MAPPING] Normalized hours: {list(normalized_hours.keys())}")
    print(f"[DEBUG-MAPPING] ================================")

    # ✅ Construir diccionario con TODAS las columnas
    result = {
        "id": row.get("id"),
        "name": name,
        "category": row.get("category") or None,
        "products": json.dumps(products) if products is not None else None,
        "priority": _ss_to_int(row.get("priority")),
        "cashback": cashback_bool,
        "address": row.get("address") or None,
        "lat": _ss_to_float(row.get("lat")),
        "lng": _ss_to_float(row.get("lon")),  # ✅ FIX: lon → lng
        "afiliado": affiliate_bool,
        "imagen_url": (row.get("cover_image_url") or None),
        "hours": json.dumps(hours) if hours is not None else None,
    }
    
    # ✅ Agregar columnas individuales de horarios
    result.update(normalized_hours)
    
    return result

# === COALESCE por tipo: evita pisar con NULL/"" y mantiene fotos de la BD si Sheet viene vacío ===
def _ss_coalesce_expr(col: str) -> str:
    if col in ("lat","lng"):
        return f"COALESCE(%({col})s::double precision, {col})"
    if col in ("priority",):
        return f"COALESCE(%({col})s::integer, {col})"
    if col in ("cashback","afiliado"):
        # CAMBIO IMPORTANTE: Para booleanos, siempre actualizar con el valor del Sheet
        # No usar COALESCE para permitir cambiar de true a false
        return f"%({col})s::boolean"
    if col in ("products","hours"):
        return f"COALESCE(%({col})s::jsonb, {col})"
    
    # ✅ NUEVO: Columnas de horarios (mon_open, tue_open, etc.)
    if col.endswith("_open") or col.endswith("_close"):
        # Si viene NULL desde el Sheet, mantener el valor de la BD
        # Si viene un valor, actualizarlo
        return f"COALESCE(%({col})s::text, {col})"
    
    # texto: no pisar con "" -> NULLIF(...,'')
    return f"COALESCE(NULLIF(%({col})s::text, ''), {col})"

def _ss_build_update(keys):
    sets, diffs = [], []
    for k in keys:
        if k == "id": continue
        
        # Para booleanos, manejar NULL especialmente
        if k in ("cashback", "afiliado"):
            # Si el valor es NULL, no actualizar
            sets.append(f"""
                {k} = CASE 
                    WHEN %({k})s::boolean IS NULL THEN {k}
                    ELSE %({k})s::boolean
                END
            """)
            diffs.append(f"(%({k})s::boolean IS DISTINCT FROM {k})")
        else:
            expr = _ss_coalesce_expr(k)
            sets.append(f"{k} = {expr}")
            diffs.append(f"({expr} IS DISTINCT FROM {k})")
    
    if not sets:
        return "SELECT 0"
    
    return f"""
        UPDATE public.places
           SET {', '.join(sets)}
         WHERE id = %(id)s
           AND ({' OR '.join(diffs)})
         RETURNING id;
    """

def _ss_build_insert(keys):
    cols = [k for k in keys if k != "id"]
    columns = ", ".join(["id"] + cols)
    values = ", ".join(["%(id)s"] + [f"%({k})s" for k in cols])
    return f"""
        INSERT INTO public.places ({columns})
        VALUES ({values})
        ON CONFLICT (id) DO NOTHING
        RETURNING id;
    """

@app.post("/sheet/sync")
async def sheet_sync(payload: Dict[str, Any] = Body(...)):
    if not SHEET_SYNC_SECRET:
        raise HTTPException(status_code=500, detail="SHEET_SYNC_SECRET no configurado")
    secret = (payload or {}).get("secret")
    if secret != SHEET_SYNC_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # filtra llaves a las aceptadas
    raw = (payload or {}).get("row") or {}

    # DEBUG TEMPORAL - AGREGA ESTAS 5 LÍNEAS AQUÍ
    print("=" * 60)
    print(f"[DEBUG-CASHBACK] RAW cashback: {raw.get('cashback')} - tipo: {type(raw.get('cashback'))}")
    print(f"[DEBUG-CASHBACK] RAW affiliate: {raw.get('affiliate')} - tipo: {type(raw.get('affiliate'))}")
    print(f"[DEBUG-CASHBACK] Columnas recibidas: {list(raw.keys())}")
    print("=" * 60)

    row = {k: v for k, v in raw.items() if k in _SHEET_ALLOWED}

    _ss_validate_min(row)
    mapped = _ss_map_to_places(row)

    # Sólo setear columnas presentes en mapped (None significa "no pisar": lo maneja COALESCE)
    keys = [k for k in mapped.keys() if k != "id"]

    try:
        with get_pool().connection() as conn, conn.cursor() as cur:
            # UPDATE si cambia algo
            upd = _ss_build_update(["id"] + keys)
            cur.execute(upd, mapped)
            updated = (cur.fetchone() is not None) if cur.description else False
            if updated:
                print(f"[sheet-sync] updated id={mapped['id']}")
                return {"status": "updated", "id": mapped["id"]}

            # INSERT si no existe
            ins = _ss_build_insert(["id"] + keys)
            cur.execute(ins, mapped)
            inserted = (cur.fetchone() is not None) if cur.description else False
            if inserted:
                print(f"[sheet-sync] inserted id={mapped['id']}")
                return {"status": "inserted", "id": mapped["id"]}

            # Sin cambios
            print(f"[sheet-sync] unchanged id={mapped['id']}")
            return {"status": "unchanged", "id": mapped["id"]}
    except Exception as e:
        print(f"[sheet-sync] ERROR id={mapped.get('id')}: {e}")
        raise HTTPException(status_code=500, detail="sync_failed")