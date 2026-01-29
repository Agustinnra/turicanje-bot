"""
Handler de Menú con Presupuesto.
Permite buscar productos con filtro de presupuesto y personas.

Ejemplo: "cervezas para 4, tenemos 600 pesos"
"""
from typing import Optional, List, Dict, Any
import psycopg.rows

# Dependencias (se inicializan desde app.py)
pool_getter = None
send_message = None


def init(get_pool_func, send_msg_func):
    """Inicializa las dependencias del módulo."""
    global pool_getter, send_message
    pool_getter = get_pool_func
    send_message = send_msg_func


async def search_menu_with_budget(
    producto: str, 
    presupuesto: int, 
    personas: int = 1,
    limit: int = 5
) -> List[Dict[str, Any]]:
    """
    Busca productos en menu_items que quepan en el presupuesto.
    
    Args:
        producto: Qué buscar (ej: "cerveza", "hamburguesa")
        presupuesto: Presupuesto total en pesos
        personas: Número de personas
        limit: Máximo de resultados
    
    Returns:
        Lista de productos con cálculos de cantidad
    """
    try:
        pool = pool_getter()
        if not pool:
            print("[MENU-BUDGET] ❌ No hay conexión a BD")
            return []
        
        presupuesto_por_persona = presupuesto / personas if personas > 0 else presupuesto
        
        # Buscar productos que contengan el término
        sql = """
        SELECT 
            m.id,
            m.nombre,
            m.precio,
            m.categoria,
            m.place_id,
            p.name as negocio,
            p.address,
            p.cashback
        FROM menu_items m
        JOIN places p ON m.place_id = p.id
        WHERE m.disponible = true
          AND m.nombre ILIKE %s
          AND m.precio <= %s
        ORDER BY 
            CASE WHEN p.cashback = true THEN 0 ELSE 1 END ASC,
            m.precio ASC
        LIMIT %s;
        """
        
        search_pattern = f"%{producto}%"
        
        print(f"[MENU-BUDGET] Buscando '{producto}' con presupuesto ${presupuesto} para {personas} personas")
        print(f"[MENU-BUDGET] Precio máximo por persona: ${presupuesto_por_persona:.2f}")
        
        with pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql, (search_pattern, presupuesto, limit))
                rows = cur.fetchall()
        
        results = []
        for row in rows:
            precio = float(row['precio'])
            cantidad_total = int(presupuesto / precio)
            cantidad_por_persona = cantidad_total // personas if personas > 0 else cantidad_total
            
            results.append({
                'id': row['id'],
                'nombre': row['nombre'],
                'precio': precio,
                'categoria': row['categoria'],
                'place_id': row['place_id'],
                'negocio': row['negocio'],
                'address': row['address'],
                'cashback': row['cashback'],
                'cantidad_total': cantidad_total,
                'cantidad_por_persona': cantidad_por_persona,
                'gasto_total': precio * cantidad_total,
                'sobra': presupuesto - (precio * cantidad_total)
            })
        
        print(f"[MENU-BUDGET] ✅ Encontrados {len(results)} productos")
        return results
        
    except Exception as e:
        print(f"[MENU-BUDGET] ❌ Error: {e}")
        return []


def format_budget_response(
    results: List[Dict[str, Any]], 
    producto: str,
    presupuesto: int,
    personas: int
) -> str:
    """
    Formatea la respuesta del bot con los resultados de presupuesto.
    """
    if not results:
        return f"""😕 No encontré *{producto}* dentro de tu presupuesto de ${presupuesto:,} para {personas} personas.

💡 *Sugerencias:*
- Intenta con un presupuesto mayor
- Busca otro producto
- Escribe solo "{producto}" para ver opciones sin límite de precio"""
    
    # Agrupar por negocio
    negocios = {}
    for r in results:
        negocio = r['negocio']
        if negocio not in negocios:
            negocios[negocio] = {
                'address': r['address'],
                'cashback': r['cashback'],
                'productos': []
            }
        negocios[negocio]['productos'].append(r)
    
    # Construir respuesta
    lines = [f"🍽️ *{producto.capitalize()}* para {personas} personas con ${presupuesto:,}\n"]
    
    for negocio, data in negocios.items():
        cashback_badge = " 💰" if data['cashback'] else ""
        lines.append(f"📍 *{negocio}*{cashback_badge}")
        
        for p in data['productos'][:3]:  # Máximo 3 por negocio
            lines.append(f"   • {p['nombre']}: ${p['precio']:.0f}")
            lines.append(f"     → Alcanzan *{p['cantidad_total']}* ({p['cantidad_por_persona']} c/u)")
        
        lines.append("")  # Línea vacía entre negocios
    
    # Agregar mejor opción
    mejor = max(results, key=lambda x: x['cantidad_total'])
    lines.append(f"✅ *Mejor opción:* {mejor['nombre']} en {mejor['negocio']}")
    lines.append(f"   ${mejor['precio']:.0f} × {mejor['cantidad_total']} = ${mejor['gasto_total']:.0f}")
    if mejor['sobra'] > 0:
        lines.append(f"   💵 Te sobran ${mejor['sobra']:.0f}")
    
    return "\n".join(lines)


async def handle_budget_search(
    wa_id: str,
    producto: str,
    presupuesto: int,
    personas: int,
    phone_number_id: str = None
):
    """
    Handler principal para búsqueda con presupuesto.
    """
    print(f"[MENU-BUDGET] Usuario {wa_id[:6]}***: '{producto}' para {personas} con ${presupuesto}")
    
    results = await search_menu_with_budget(producto, presupuesto, personas)
    
    response = format_budget_response(results, producto, presupuesto, personas)
    
    await send_message(wa_id, response, phone_number_id)
    
    print(f"[MENU-BUDGET] ✅ Respuesta enviada a {wa_id[:6]}***")