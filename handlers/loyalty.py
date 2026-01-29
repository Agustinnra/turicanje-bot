"""
Handler de Loyalty - Consulta de puntos y código QR.
"""
from typing import Optional, Tuple
import psycopg.rows

# Estas se importarán desde app.py
pool_getter = None
send_message = None
send_image = None


def init(get_pool_func, send_msg_func, send_img_func):
    """Inicializa las dependencias del módulo."""
    global pool_getter, send_message, send_image
    pool_getter = get_pool_func
    send_message = send_msg_func
    send_image = send_img_func


def normalize_phone_for_search(wa_id: str) -> list:
    """Normaliza el teléfono de WhatsApp para buscar en BD."""
    telefono = wa_id.strip()
    
    if telefono.startswith('521') and len(telefono) == 13:
        telefono_10 = telefono[3:]
    elif telefono.startswith('52') and len(telefono) == 12:
        telefono_10 = telefono[2:]
    else:
        telefono_10 = telefono[-10:] if len(telefono) >= 10 else telefono
    
    return [
        telefono,
        telefono_10,
        '52' + telefono_10,
        '521' + telefono_10,
        '+52' + telefono_10,
        '+521' + telefono_10,
    ]


async def get_loyalty_user_by_phone(wa_id: str) -> Optional[dict]:
    """Busca un usuario en loyalty_users por su teléfono."""
    try:
        pool = pool_getter()
        if not pool:
            print("[LOYALTY] ❌ No hay conexión a BD")
            return None
        
        variaciones = normalize_phone_for_search(wa_id)
        placeholders = ', '.join(['%s'] * len(variaciones))
        
        sql = f"""
        SELECT 
            id, telefono, nombre, email, codigo_qr,
            saldo_puntos, suscripcion_activa,
            suscripcion_fecha_vencimiento, created_at
        FROM loyalty_users
        WHERE telefono IN ({placeholders})
        LIMIT 1;
        """
        
        print(f"[LOYALTY] Buscando teléfono en variaciones: {variaciones[:3]}...")
        
        with pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql, tuple(variaciones))
                user = cur.fetchone()
        
        if user:
            print(f"[LOYALTY] ✅ Usuario encontrado: {user.get('nombre', 'Sin nombre')} - {user.get('saldo_puntos', 0)} puntos")
            return dict(user)
        else:
            print(f"[LOYALTY] ⚠️ Usuario no encontrado para {wa_id[:6]}***")
            return None
            
    except Exception as e:
        print(f"[LOYALTY] ❌ Error buscando usuario: {e}")
        return None


async def handle_loyalty_points_query(wa_id: str, phone_number_id: str = None):
    """Maneja cuando el usuario pregunta por sus puntos/saldo."""
    user = await get_loyalty_user_by_phone(wa_id)
    
    if user:
        nombre = user.get('nombre') or 'amigo'
        puntos = user.get('saldo_puntos') or 0
        suscripcion_activa = user.get('suscripcion_activa', False)
        fecha_vencimiento = user.get('suscripcion_fecha_vencimiento')
        
        puntos_formateados = f"{int(puntos):,}".replace(',', ',')
        
        if suscripcion_activa:
            estado_emoji = "✅"
            estado_texto = "activa"
            if fecha_vencimiento:
                fecha_str = fecha_vencimiento.strftime("%d/%m/%Y") if hasattr(fecha_vencimiento, 'strftime') else str(fecha_vencimiento)[:10]
                estado_extra = f"\n📅 Vigente hasta: {fecha_str}"
            else:
                estado_extra = ""
        else:
            estado_emoji = "⚠️"
            estado_texto = "inactiva (puntos congelados)"
            estado_extra = "\n\n💡 Renueva tu suscripción para usar tus puntos"
        
        mensaje = f"""💰 *Hola {nombre}!*

Tu saldo actual es:
🎯 *{puntos_formateados} puntos*

📊 Membresía: {estado_emoji} {estado_texto}{estado_extra}

💡 Acumula puntos comprando en comercios Turicanje y canjéalos por descuentos.

¿Necesitas tu código QR? Escribe *"mi qr"* 📱"""
        
        await send_message(wa_id, mensaje, phone_number_id)
        print(f"[LOYALTY] ✅ Saldo enviado a {wa_id[:6]}***: {puntos} puntos")
        
    else:
        mensaje = """👋 *¡Hola!*

Aún no estás registrado en el programa de puntos de Turicanje.

✨ *Beneficios de unirte:*
- Acumula puntos en cada compra
- Canjea por descuentos
- Primer año GRATIS

📱 Regístrate en: turicanje.com/suscripcion

¿Dudas? Escríbenos a soporte@turicanje.com"""
        
        await send_message(wa_id, mensaje, phone_number_id)
        print(f"[LOYALTY] ⚠️ Usuario no registrado: {wa_id[:6]}***")


async def handle_loyalty_qr_query(wa_id: str, phone_number_id: str = None):
    """Maneja cuando el usuario pide su código QR."""
    user = await get_loyalty_user_by_phone(wa_id)
    
    if user:
        codigo_qr = user.get('codigo_qr')
        nombre = user.get('nombre') or 'amigo'
        puntos = user.get('saldo_puntos') or 0
        
        if codigo_qr:
            qr_image_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={codigo_qr}&bgcolor=ffffff&color=d1007d"
            
            mensaje = f"""📱 *Tu código QR, {nombre}!*

🔑 Código: *{codigo_qr}*
💰 Puntos: *{int(puntos):,}*

Muestra este QR en cualquier comercio Turicanje para:
✅ Acumular puntos
✅ Canjear descuentos

👇 *Tu QR:*"""
            
            await send_message(wa_id, mensaje, phone_number_id)
            await send_image(wa_id, qr_image_url, caption=f"🎯 {codigo_qr}", phone_number_id=phone_number_id)
            
            print(f"[LOYALTY] ✅ QR enviado a {wa_id[:6]}***: {codigo_qr}")
            
        else:
            mensaje = f"""Hola {nombre}! 👋

Tu cuenta no tiene un código QR asignado aún.

Por favor contacta a soporte@turicanje.com para generarte uno.

Mientras tanto, en los comercios pueden buscarte por tu número de teléfono 📱"""
            
            await send_message(wa_id, mensaje, phone_number_id)
            print(f"[LOYALTY] ⚠️ Usuario sin código QR: {wa_id[:6]}***")
            
    else:
        mensaje = """👋 *¡Hola!*

Aún no estás registrado en el programa de puntos de Turicanje, por eso no tienes código QR.

✨ *Regístrate para obtener:*
- Tu código QR personal
- Acumulación de puntos
- Descuentos exclusivos
- ¡Primer año GRATIS!

📱 Regístrate en: turicanje.com/suscripcion"""
        
        await send_message(wa_id, mensaje, phone_number_id)
        print(f"[LOYALTY] ⚠️ QR solicitado pero usuario no registrado: {wa_id[:6]}***")


def is_loyalty_query(text: str) -> Tuple[bool, Optional[str]]:
    """
    Detecta si el mensaje es una consulta de puntos o QR.
    Retorna: (es_loyalty_query, tipo) donde tipo es 'points' o 'qr' o None
    """
    text_lower = text.lower().strip()
    
    points_keywords = [
        'mis puntos', 'mi puntos', 'puntos', 'mi saldo', 'saldo',
        'cuantos puntos', 'cuántos puntos', 'tengo puntos',
        'cashback', 'mi cashback', 'ver puntos', 'consultar puntos',
        'cuanto tengo', 'cuánto tengo'
    ]
    
    qr_keywords = [
        'mi qr', 'mi código', 'mi codigo', 'código qr', 'codigo qr',
        'ver qr', 'mostrar qr', 'enviar qr', 'manda mi qr', 'mandame mi qr',
        'quiero mi qr', 'necesito mi qr', 'dame mi qr'
    ]
    
    for keyword in qr_keywords:
        if keyword in text_lower:
            return (True, 'qr')
    
    for keyword in points_keywords:
        if keyword in text_lower:
            return (True, 'points')
    
    return (False, None)