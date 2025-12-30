"""
============================================
TURICANJE - ANALYTICS FUNCTIONS
============================================
Funciones para guardar data automáticamente en:
1. conversation_raw (TODO)
2. analytics_dashboard (TOP 20 métricas)

Con filtro de números excluidos para testing.
============================================
"""

import os
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pytz
import psycopg
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
# FUNCIÓN: Guardar evento RAW
# ============================================

async def save_raw_event(
    event_type: str,
    wa_id: str,
    session_id: str,
    data: Dict[str, Any],
    pool: ConnectionPool
) -> bool:
    """
    Guarda un evento en la tabla conversation_raw.
    
    Args:
        event_type: Tipo de evento ('message', 'search', 'click', 'location', 'session_start', 'session_end', 'pagination')
        wa_id: WhatsApp ID del usuario
        session_id: ID de la sesión
        data: Diccionario con TODA la data del evento
        pool: Connection pool de PostgreSQL
    
    Returns:
        bool: True si se guardó exitosamente, False si falló o usuario excluido
    """
    # ✅ FILTRO: No guardar si está excluido
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        
        conn = await pool.getconn()
        try:
            await conn.execute(
                """
                INSERT INTO conversation_raw (event_type, wa_id, session_id, timestamp, raw_data)
                VALUES ($1, $2, $3, $4, $5)
                """,
                (event_type, wa_id, session_id, now, json.dumps(data))
            )
            await conn.commit()
            print(f"[ANALYTICS] ✅ RAW guardado: {event_type} | {wa_id}")
            return True
        finally:
            await pool.putconn(conn)
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error guardando RAW: {e}")
        return False


# ============================================
# FUNCIÓN: Incrementar métrica diaria
# ============================================

async def increment_metric(
    date: str,
    metric: str,
    value: int,
    pool: ConnectionPool
) -> bool:
    """
    Incrementa una métrica específica en analytics_dashboard.
    
    Args:
        date: Fecha en formato 'YYYY-MM-DD'
        metric: Nombre de la métrica (ej: 'total_searches', 'total_clicks')
        value: Valor a incrementar (usualmente 1)
        pool: Connection pool de PostgreSQL
    
    Returns:
        bool: True si se actualizó exitosamente
    """
    try:
        conn = await pool.getconn()
        try:
            # Upsert: insert o update si ya existe
            await conn.execute(
                f"""
                INSERT INTO analytics_dashboard (date, {metric})
                VALUES ($1, $2)
                ON CONFLICT (date) 
                DO UPDATE SET 
                    {metric} = analytics_dashboard.{metric} + EXCLUDED.{metric},
                    updated_at = NOW()
                """,
                (date, value)
            )
            await conn.commit()
            print(f"[ANALYTICS] ✅ Métrica incrementada: {metric} +{value} ({date})")
            return True
        finally:
            await pool.putconn(conn)
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error incrementando métrica: {e}")
        return False


# ============================================
# FUNCIÓN: Actualizar usuario único
# ============================================

async def update_unique_user(
    wa_id: str,
    pool: ConnectionPool
) -> bool:
    """
    Actualiza o crea registro de usuario único.
    
    Args:
        wa_id: WhatsApp ID
        pool: Connection pool
    
    Returns:
        bool: True si es usuario nuevo, False si es returning
    """
    # ✅ FILTRO: No guardar si está excluido
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        
        conn = await pool.getconn()
        try:
            # Verificar si existe
            result = await conn.execute(
                "SELECT wa_id FROM users_unique WHERE wa_id = $1",
                (wa_id,)
            )
            row = await result.fetchone()
            
            is_new_user = row is None
            
            if is_new_user:
                # Insertar nuevo usuario
                await conn.execute(
                    """
                    INSERT INTO users_unique (wa_id, first_seen, last_seen)
                    VALUES ($1, $2, $3)
                    """,
                    (wa_id, now, now)
                )
                print(f"[ANALYTICS] 🆕 Nuevo usuario: {wa_id}")
            else:
                # Actualizar last_seen
                await conn.execute(
                    "UPDATE users_unique SET last_seen = $1 WHERE wa_id = $2",
                    (now, wa_id)
                )
            
            await conn.commit()
            return is_new_user
        finally:
            await pool.putconn(conn)
            
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error actualizando usuario único: {e}")
        return False


# ============================================
# FUNCIÓN: Registrar búsqueda
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
    """
    Registra una búsqueda completa.
    
    Guarda en:
    1. conversation_raw (data completa)
    2. analytics_dashboard (incrementa total_searches)
    """
    # ✅ FILTRO
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        today = now.strftime("%Y-%m-%d")
        
        # Data completa para RAW
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
        
        # Guardar en RAW
        await save_raw_event("search", wa_id, session_id, raw_data, pool)
        
        # Incrementar métricas
        await increment_metric(today, "total_searches", 1, pool)
        
        # Actualizar avg_results_per_search (se recalcula al final del día)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging search: {e}")
        return False


# ============================================
# FUNCIÓN: Registrar click
# ============================================

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
    distance_meters: Optional[int],
    was_open: bool,
    pool: ConnectionPool
) -> bool:
    """
    Registra un click en un lugar.
    
    Guarda en:
    1. conversation_raw (data completa)
    2. analytics_dashboard (incrementa total_clicks, affiliate_clicks, cashback_clicks)
    """
    # ✅ FILTRO
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        today = now.strftime("%Y-%m-%d")
        
        # Data completa para RAW
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
        
        # Guardar en RAW
        await save_raw_event("click", wa_id, session_id, raw_data, pool)
        
        # Incrementar métricas
        await increment_metric(today, "total_clicks", 1, pool)
        
        if is_affiliate:
            await increment_metric(today, "affiliate_clicks", 1, pool)
        
        if has_cashback:
            await increment_metric(today, "cashback_place_clicks", 1, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging click: {e}")
        return False


# ============================================
# FUNCIÓN: Registrar sesión iniciada
# ============================================

async def log_session_start(
    wa_id: str,
    session_id: str,
    is_new_user: bool,
    pool: ConnectionPool
) -> bool:
    """Registra inicio de sesión"""
    # ✅ FILTRO
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
        
        await save_raw_event("session_start", wa_id, session_id, raw_data, pool)
        
        # Incrementar DAU
        await increment_metric(today, "daily_active_users", 1, pool)
        
        if is_new_user:
            await increment_metric(today, "new_users", 1, pool)
        else:
            await increment_metric(today, "returning_users", 1, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging session start: {e}")
        return False


# ============================================
# FUNCIÓN: Registrar sesión terminada
# ============================================

async def log_session_end(
    wa_id: str,
    session_id: str,
    duration_sec: int,
    message_count: int,
    search_count: int,
    shown_count: int,
    clicked_link: bool,
    ended_by: str,
    pool: ConnectionPool
) -> bool:
    """Registra fin de sesión con métricas completas"""
    # ✅ FILTRO
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        
        raw_data = {
            "session_id": session_id,
            "duration_sec": duration_sec,
            "message_count": message_count,
            "search_count": search_count,
            "shown_count": shown_count,
            "clicked_link": clicked_link,
            "ended_by": ended_by,  # 'timeout', 'user_goodbye', 'inactive'
            "timestamp": now.isoformat()
        }
        
        await save_raw_event("session_end", wa_id, session_id, raw_data, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging session end: {e}")
        return False


# ============================================
# FUNCIÓN: Registrar ubicación compartida
# ============================================

async def log_location_shared(
    wa_id: str,
    session_id: str,
    lat: float,
    lng: float,
    pool: ConnectionPool
) -> bool:
    """Registra cuando usuario comparte ubicación"""
    # ✅ FILTRO
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        today = now.strftime("%Y-%m-%d")
        
        raw_data = {
            "lat": lat,
            "lng": lng,
            "timestamp": now.isoformat()
        }
        
        await save_raw_event("location", wa_id, session_id, raw_data, pool)
        
        # La tasa de location_share se calcula al final del día
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging location: {e}")
        return False


# ============================================
# FUNCIÓN: Registrar paginación
# ============================================

async def log_pagination(
    wa_id: str,
    session_id: str,
    page_number: int,
    search_craving: str,
    pool: ConnectionPool
) -> bool:
    """Registra cuando usuario pide 'más' opciones"""
    # ✅ FILTRO
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        
        raw_data = {
            "page_number": page_number,
            "search_craving": search_craving,
            "timestamp": now.isoformat()
        }
        
        await save_raw_event("pagination", wa_id, session_id, raw_data, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging pagination: {e}")
        return False


# ============================================
# FUNCIÓN: Registrar despedida automática
# ============================================

async def log_goodbye_sent(
    wa_id: str,
    session_id: str,
    clicked_link: bool,
    pool: ConnectionPool
) -> bool:
    """Registra cuando se envía mensaje de despedida automático"""
    # ✅ FILTRO
    if is_excluded_user(wa_id):
        return False
    
    try:
        now = datetime.now(TZ)
        today = now.strftime("%Y-%m-%d")
        
        raw_data = {
            "clicked_link": clicked_link,
            "timestamp": now.isoformat()
        }
        
        await save_raw_event("goodbye", wa_id, session_id, raw_data, pool)
        
        await increment_metric(today, "goodbye_messages_sent", 1, pool)
        
        return True
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error logging goodbye: {e}")
        return False


# ============================================
# FUNCIÓN: Calcular métricas del día
# ============================================

async def calculate_daily_metrics(date: str, pool: ConnectionPool) -> bool:
    """
    Calcula métricas agregadas para un día específico.
    Se ejecuta al final del día o bajo demanda.
    """
    try:
        conn = await pool.getconn()
        try:
            await conn.execute(
                "SELECT calculate_daily_metrics($1)",
                (date,)
            )
            await conn.commit()
            print(f"[ANALYTICS] ✅ Métricas calculadas para: {date}")
            return True
        finally:
            await pool.putconn(conn)
        
    except Exception as e:
        print(f"[ANALYTICS] ❌ Error calculando métricas: {e}")
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
    print("📊 ANALYTICS SYSTEM INITIALIZED")
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