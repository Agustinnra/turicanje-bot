"""
============================================
TURICANJE - ANALYTICS FUNCTIONS (SYNC VERSION)
============================================
Funciones para guardar data automáticamente en:
1. conversation_raw (TODO)
2. analytics_dashboard (TOP 20 métricas)

Con filtro de números excluidos para testing.

NOTA: Versión SÍNCRONA compatible con ConnectionPool síncrono
============================================
"""

import os
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pytz
from psycopg_pool import ConnectionPool

# ============================================
# CONFIGURACIÓN
# ============================================

# Números excluidos (de .env)
EXCLUDED_PHONE_NUMBERS = os.getenv("EXCLUDED_PHONE_NUMBERS", "").split(",")
EXCLUDED_PHONE_NUMBERS = [num.strip() for num in EXCLUDED_PHONE_NUMBERS if num.strip()]

# Timezone
TZ = pytz.timezone("America/Mexico_City")

# ============================================
# FUNCIÓN: Verificar si usuario está excluido
# ============================================

def is_excluded_user(wa_id: str) -> bool:
    """
    Verifica si el usuario está en la lista de exclusión.
    Usuarios excluidos: NO se guarda ninguna data (para testing).
    
    Returns:
        bool: True si está excluido, False si es usuario real
    """
    excluded = wa_id in EXCLUDED_PHONE_NUMBERS
    
    if excluded:
        print(f"[ANALYTICS] ⚠️  Usuario EXCLUIDO: {wa_id} - Data NO se guardará")
    
    return excluded


# ============================================
# FUNCIÓN: Guardar evento RAW (SYNC)
# ============================================

def save_raw_event_sync(
    event_type: str,
    wa_id: str,
    session_id: str,
    data: Dict[str, Any],
    pool: ConnectionPool
) -> bool:
    """
    Guarda un evento en la tabla conversation_raw (VERSIÓN SÍNCRONA).
    
    Args:
        event_type: Tipo de evento
        wa_id: WhatsApp ID del usuario
        session_id: ID de la sesión
        data: Diccionario con TODA la data del evento
        pool: Connection pool de PostgreSQL
    
    Returns:
        bool: True si se guardó exitosamente
    """
    # ✅ FILTRO: No guardar si está excluido
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conversation_raw (event_type, wa_id, session_id, timestamp, raw_data)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (event_type, wa_id, session_id, now, json.dumps(data))
                )
        
        print(f"[ANALYTICS] ✅ RAW guardado: {event_type} | {wa_id}")
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error guardando RAW: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================
# FUNCIÓN: Incrementar métrica diaria (SYNC)
# ============================================

def increment_metric_sync(
    date: str,
    metric: str,
    value: int,
    pool: ConnectionPool
) -> bool:
    """
    Incrementa una métrica específica en analytics_dashboard (VERSIÓN SÍNCRONA).
    """
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Upsert: insert o update si ya existe
                cur.execute(
                    f"""
                    INSERT INTO analytics_dashboard (date, {metric})
                    VALUES (%s, %s)
                    ON CONFLICT (date) 
                    DO UPDATE SET 
                        {metric} = analytics_dashboard.{metric} + EXCLUDED.{metric},
                        updated_at = NOW()
                    """,
                    (date, value)
                )
        
        print(f"[ANALYTICS] ✅ Métrica incrementada: {metric} +{value} ({date})")
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error incrementando métrica: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================
# FUNCIÓN: Actualizar usuario único (SYNC)
# ============================================

def update_unique_user_sync(
    wa_id: str,
    pool: ConnectionPool
) -> bool:
    """
    Actualiza o crea registro de usuario único (VERSIÓN SÍNCRONA).
    """
    # ✅ FILTRO: No guardar si está excluido
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Verificar si existe
                cur.execute(
                    "SELECT wa_id FROM users_unique WHERE wa_id = %s",
                    (wa_id,)
                )
                row = cur.fetchone()
                
                is_new_user = row is None
                
                if is_new_user:
                    # Insertar nuevo usuario
                    cur.execute(
                        """
                        INSERT INTO users_unique (wa_id, first_seen, last_seen)
                        VALUES (%s, %s, %s)
                        """,
                        (wa_id, now, now)
                    )
                    print(f"[ANALYTICS] 🆕 Nuevo usuario: {wa_id}")
                else:
                    # Actualizar last_seen
                    cur.execute(
                        "UPDATE users_unique SET last_seen = %s WHERE wa_id = %s",
                        (now, wa_id)
                    )
                
                return is_new_user
            
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error actualizando usuario único: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================
# WRAPPERS ASYNC (para compatibilidad)
# ============================================

async def save_raw_event(event_type, wa_id, session_id, data, pool):
    """Wrapper async que llama a la versión sync"""
    return save_raw_event_sync(event_type, wa_id, session_id, data, pool)

async def increment_metric(date, metric, value, pool):
    """Wrapper async que llama a la versión sync"""
    return increment_metric_sync(date, metric, value, pool)

async def update_unique_user(wa_id, pool):
    """Wrapper async que llama a la versión sync"""
    return update_unique_user_sync(wa_id, pool)


# ============================================
# FUNCIONES DE LOGGING (usando wrappers sync)
# ============================================

async def log_search(
    wa_id: str,
    session_id: str,
    craving: str,
    had_location: bool,
    user_lat: Optional[float],
    user_lng: Optional[float],
    results_count: int,
    shown_count: int,
    used_expansion: bool,
    expanded_terms: List[str],
    db_query_time_ms: int,
    pool: ConnectionPool
) -> bool:
    """Registra una búsqueda completa"""
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        today = now.strftime("%Y-%m-%d")
        
        raw_data = {
            "craving": craving,
            "craving_normalized": craving.lower().strip(),
            "had_location": had_location,
            "user_lat": user_lat,
            "user_lng": user_lng,
            "results_count": results_count,
            "shown_count": shown_count,
            "used_expansion": used_expansion,
            "expanded_terms": expanded_terms,
            "db_query_time_ms": db_query_time_ms,
            "timestamp": now.isoformat(),
            "hour": now.hour,
            "day_of_week": now.strftime("%A"),
            "is_weekend": now.weekday() >= 5
        }
        
        save_raw_event_sync("search", wa_id, session_id, raw_data, pool)
        increment_metric_sync(today, "total_searches", 1, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging search: {e}")
        return False


async def log_click(
    wa_id: str,
    session_id: str,
    search_craving: str,
    place_id: str,
    place_name: str,
    place_category: str,
    has_cashback: bool,
    is_affiliate: bool,
    has_delivery: bool,
    result_position: int,
    distance_meters: Optional[float],
    was_open: bool,
    pool: ConnectionPool
) -> bool:
    """Registra un click en un lugar"""
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        today = now.strftime("%Y-%m-%d")
        
        raw_data = {
            "search_craving": search_craving,
            "place_id": place_id,
            "place_name": place_name,
            "place_category": place_category,
            "has_cashback": has_cashback,
            "is_affiliate": is_affiliate,
            "has_delivery": has_delivery,
            "result_position": result_position,
            "distance_meters": distance_meters,
            "was_open": was_open,
            "timestamp": now.isoformat(),
            "hour": now.hour
        }
        
        save_raw_event_sync("click", wa_id, session_id, raw_data, pool)
        increment_metric_sync(today, "total_clicks", 1, pool)
        
        if is_affiliate:
            increment_metric_sync(today, "affiliate_clicks", 1, pool)
        
        if has_cashback:
            increment_metric_sync(today, "cashback_place_clicks", 1, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging click: {e}")
        return False


async def log_session_start(
    wa_id: str,
    session_id: str,
    is_new_user: bool,
    pool: ConnectionPool
) -> bool:
    """Registra inicio de sesión"""
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        today = now.strftime("%Y-%m-%d")
        
        raw_data = {
            "session_id": session_id,
            "is_new_user": is_new_user,
            "timestamp": now.isoformat()
        }
        
        save_raw_event_sync("session_start", wa_id, session_id, raw_data, pool)
        increment_metric_sync(today, "daily_active_users", 1, pool)
        
        if is_new_user:
            increment_metric_sync(today, "new_users", 1, pool)
        else:
            increment_metric_sync(today, "returning_users", 1, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging session start: {e}")
        return False


async def log_location_shared(
    wa_id: str,
    session_id: str,
    lat: float,
    lng: float,
    pool: ConnectionPool
) -> bool:
    """Registra cuando usuario comparte ubicación"""
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        
        raw_data = {
            "lat": lat,
            "lng": lng,
            "timestamp": now.isoformat()
        }
        
        save_raw_event_sync("location", wa_id, session_id, raw_data, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging location: {e}")
        return False


async def log_pagination(
    wa_id: str,
    session_id: str,
    page_number: int,
    search_craving: str,
    pool: ConnectionPool
) -> bool:
    """Registra cuando usuario pide 'más' opciones"""
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        
        raw_data = {
            "page_number": page_number,
            "search_craving": search_craving,
            "timestamp": now.isoformat()
        }
        
        save_raw_event_sync("pagination", wa_id, session_id, raw_data, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging pagination: {e}")
        return False


async def log_goodbye_sent(
    wa_id: str,
    session_id: str,
    clicked_link: bool,
    pool: ConnectionPool
) -> bool:
    """Registra cuando se envía mensaje de despedida automático"""
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        today = now.strftime("%Y-%m-%d")
        
        raw_data = {
            "clicked_link": clicked_link,
            "timestamp": now.isoformat()
        }
        
        save_raw_event_sync("goodbye", wa_id, session_id, raw_data, pool)
        increment_metric_sync(today, "goodbye_messages_sent", 1, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging goodbye: {e}")
        return False


# ============================================
# INICIALIZACIÓN
# ============================================

def init_analytics():
    """
    Inicializa el sistema de analytics.
    Muestra configuración en logs.
    """
    print("\n" + "="*50)
    print("📊 ANALYTICS SYSTEM INITIALIZED (SYNC VERSION)")
    print("="*50)
    print(f"🚫 Números excluidos: {len(EXCLUDED_PHONE_NUMBERS)}")
    if EXCLUDED_PHONE_NUMBERS:
        for num in EXCLUDED_PHONE_NUMBERS:
            print(f"   - {num}")
    else:
        print("   (ninguno)")
    print("✅ Analytics activo para usuarios reales")
    print("="*50 + "\n")

# Ejecutar al importar
init_analytics()