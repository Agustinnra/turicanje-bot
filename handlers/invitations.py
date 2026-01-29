"""
Handler de Invitaciones Comerciales.
Maneja cuando un comercio hace click en "Obtener mi acceso".
"""
import psycopg.rows

# Dependencias (se inicializan desde app.py)
pool_getter = None
send_message = None


def init(get_pool_func, send_msg_func):
    """Inicializa las dependencias del módulo."""
    global pool_getter, send_message
    pool_getter = get_pool_func
    send_message = send_msg_func


async def handle_invitation_button_click(wa_id: str, phone_number_id: str = None):
    """
    Maneja cuando un comercio hace click en el botón "Obtener mi acceso"
    del template de WhatsApp.
    """
    try:
        pool = pool_getter()
        if not pool:
            print(f"[INVITACION-BOT] ❌ No hay conexión a BD")
            await send_message(wa_id, "Lo siento, hay un problema técnico. Por favor intenta más tarde.", phone_number_id)
            return
        
        telefono_original = wa_id
        
        if telefono_original.startswith('521') and len(telefono_original) == 13:
            telefono_10_digitos = telefono_original[3:]
        elif telefono_original.startswith('52') and len(telefono_original) == 12:
            telefono_10_digitos = telefono_original[2:]
        else:
            telefono_10_digitos = telefono_original
        
        variaciones = [
            telefono_original,
            telefono_10_digitos,
            '52' + telefono_10_digitos,
            '521' + telefono_10_digitos,
        ]
        
        sql = """
        SELECT i.codigo, i.nombre_invitado, p.name as nombre_negocio
        FROM invitaciones_comercio i
        JOIN places p ON i.place_id = p.id
        WHERE i.telefono_invitado IN (%s, %s, %s, %s)
          AND i.usado = false
        ORDER BY i.created_at DESC
        LIMIT 1;
        """
        
        print(f"[INVITACION-BOT] Buscando teléfono en variaciones: {variaciones}")
        
        with pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql, tuple(variaciones))
                invitacion = cur.fetchone()
        
        if invitacion:
            codigo = invitacion['codigo']
            nombre = invitacion.get('nombre_invitado') or ''
            negocio = invitacion.get('nombre_negocio') or 'tu negocio'
            
            link = f"https://turicanje.com/registro-comercio?codigo={codigo}"
            
            mensaje = f"""🎉 *¡Perfecto{' ' + nombre if nombre else ''}!*

Aquí está tu acceso para administrar *{negocio}* en Turicanje:

🔑 *Código de invitación:*
{codigo}

📱 *Regístrate aquí:*
{link}

✨ *Con tu cuenta podrás:*
- Ver estadísticas de tu negocio
- Gestionar tu información
- Ofrecer cashback a clientes
- ¡Primer año GRATIS!

¿Dudas? Escríbenos a soporte@turicanje.com"""
            
            await send_message(wa_id, mensaje, phone_number_id)
            print(f"[INVITACION-BOT] ✅ Código enviado a {wa_id} para negocio: {negocio}")
            
        else:
            mensaje = """Hola 👋

No encontré una invitación pendiente para este número.

Si crees que es un error, por favor contacta a soporte@turicanje.com con el nombre de tu negocio.

¿O tal vez quieres buscar un lugar para comer? 🍽️ Solo dime qué se te antoja."""
            
            await send_message(wa_id, mensaje, phone_number_id)
            print(f"[INVITACION-BOT] ⚠️ No hay invitación pendiente para {wa_id}")
            
    except Exception as e:
        print(f"[INVITACION-BOT] ❌ Error: {e}")
        await send_message(wa_id, "Lo siento, hubo un problema. Por favor intenta de nuevo o escribe a soporte@turicanje.com", phone_number_id)