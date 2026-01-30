"""
Handler de Menú con Presupuesto.
Permite buscar productos con filtro de presupuesto y personas.
Soporta múltiples productos: "tacos y cervezas para 4 con 600 pesos"
"""
from typing import Optional, List, Dict, Any, Union
import psycopg.rows

# Dependencias (se inicializan desde app.py)
pool_getter = None
send_message = None


def init(get_pool_func, send_msg_func):
    """Inicializa las dependencias del módulo."""
    global pool_getter, send_message
    pool_getter = get_pool_func
    send_message = send_msg_func


def normalizar_producto(producto: str) -> List[str]:
    """
    Genera variaciones de búsqueda para un producto.
    Incluye singular/plural y sinónimos comunes.
    """
    producto_lower = producto.lower().strip()
    variaciones = [producto_lower]
    
    # Singular/plural
    if producto_lower.endswith('s') and len(producto_lower) > 3:
        variaciones.append(producto_lower[:-1])
    if producto_lower.endswith('es') and len(producto_lower) > 4:
        variaciones.append(producto_lower[:-2])
    if not producto_lower.endswith('s'):
        variaciones.append(producto_lower + 's')
    
    # Sinónimos comunes
    sinonimos = {
        'chela': ['cerveza', 'cervezas'],
        'chelas': ['cerveza', 'cervezas'],
        'birria': ['cerveza', 'cervezas'],
        'birrias': ['cerveza', 'cervezas'],
        'refresco': ['refresco', 'refrescos', 'soda', 'sodas'],
        'soda': ['refresco', 'refrescos', 'soda', 'sodas'],
        'hamburguesa': ['hamburguesa', 'hamburguesas', 'burger', 'burgers'],
        'burger': ['hamburguesa', 'hamburguesas', 'burger', 'burgers'],
        'papas': ['papas', 'papa', 'papas fritas', 'french fries'],
        'agua': ['agua', 'aguas', 'botella de agua'],
        'taco': ['taco', 'tacos'],
        'tacos': ['taco', 'tacos'],
        'torta': ['torta', 'tortas'],
        'pizza': ['pizza', 'pizzas'],
        'cafe': ['café', 'cafés', 'coffee'],
        'café': ['café', 'cafés', 'coffee'],
    }
    
    if producto_lower in sinonimos:
        variaciones.extend(sinonimos[producto_lower])
    
    return list(dict.fromkeys(variaciones))


async def search_menu_with_budget(
    productos: List[str], 
    presupuesto: int, 
    personas: int = 1,
    limit: int = 15
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Busca múltiples productos en menu_items que quepan en el presupuesto.
    Retorna dict con resultados por producto.
    """
    try:
        pool = pool_getter()
        if not pool:
            print("[MENU-BUDGET] ❌ No hay conexión a BD")
            return {}
        
        resultados_por_producto = {}
        
        for producto in productos:
            variaciones = normalizar_producto(producto)
            print(f"[MENU-BUDGET] Buscando '{producto}' con variaciones: {variaciones}")
            
            # Construir condiciones OR para cada variación
            conditions = " OR ".join(["m.nombre ILIKE %s" for _ in variaciones])
            patterns = [f"%{v}%" for v in variaciones]
            
            sql = f"""
            SELECT 
                m.id, m.nombre, m.precio, m.categoria, m.place_id,
                p.name as negocio, p.address, p.cashback
            FROM menu_items m
            JOIN places p ON m.place_id = p.id
            WHERE m.disponible = true
              AND m.precio <= %s
              AND ({conditions})
            ORDER BY 
                CASE WHEN p.cashback = true THEN 0 ELSE 1 END ASC,
                m.precio ASC
            LIMIT %s;
            """
            
            params = [presupuesto] + patterns + [limit]
            
            with pool.connection() as conn:
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    cur.execute(sql, tuple(params))
                    rows = cur.fetchall()
            
            # Procesar resultados
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
            
            if results:
                resultados_por_producto[producto] = results
                print(f"[MENU-BUDGET] ✅ '{producto}': {len(results)} opciones encontradas")
            else:
                print(f"[MENU-BUDGET] ❌ '{producto}': sin resultados")
        
        return resultados_por_producto
        
    except Exception as e:
        print(f"[MENU-BUDGET] ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {}


def format_budget_response_multiple(
    resultados: Dict[str, List[Dict[str, Any]]], 
    productos: List[str],
    presupuesto: int,
    personas: int
) -> str:
    """
    Formatea la respuesta del bot con múltiples productos.
    """
    if not resultados:
        productos_str = ", ".join(productos)
        return f"""😕 No encontré *{productos_str}* dentro de tu presupuesto de ${presupuesto:,} para {personas} personas.

💡 *Sugerencias:*
- Intenta con un presupuesto mayor
- Busca otros productos
- Escribe solo el nombre del producto para ver opciones sin límite de precio"""
    
    lines = [f"🍽️ *Búsqueda para {personas} personas con ${presupuesto:,}*\n"]
    
    mejor_combinacion = []
    
    for producto, items in resultados.items():
        lines.append(f"━━━ *{producto.upper()}* ━━━")
        
        # Mostrar top 3 por producto
        for item in items[:3]:
            cashback_badge = " 💰" if item['cashback'] else ""
            lines.append(f"📍 {item['negocio']}{cashback_badge}")
            lines.append(f"   • {item['nombre']}: ${item['precio']:.0f}")
            lines.append(f"   → Alcanzan *{item['cantidad_total']}* ({item['cantidad_por_persona']} c/u)")
        
        # Guardar mejor opción de cada producto
        if items:
            mejor_combinacion.append(items[0])
        
        lines.append("")
    
    # Calcular combinación sugerida
    if len(mejor_combinacion) > 1:
        lines.append("━━━ *💡 COMBINACIÓN SUGERIDA* ━━━")
        
        # Dividir presupuesto equitativamente
        presupuesto_por_producto = presupuesto // len(mejor_combinacion)
        total_gastado = 0
        
        for item in mejor_combinacion:
            cantidad = int(presupuesto_por_producto / item['precio'])
            if cantidad > 0:
                gasto = item['precio'] * cantidad
                total_gastado += gasto
                lines.append(f"• {cantidad}x {item['nombre']} (${gasto:.0f})")
        
        sobra = presupuesto - total_gastado
        if sobra > 0:
            lines.append(f"💵 Te sobran ${sobra:.0f}")
    elif mejor_combinacion:
        # Solo un producto
        mejor = mejor_combinacion[0]
        lines.append(f"✅ *Mejor opción:* {mejor['nombre']} en {mejor['negocio']}")
        lines.append(f"   ${mejor['precio']:.0f} × {mejor['cantidad_total']} = ${mejor['gasto_total']:.0f}")
        if mejor['sobra'] > 0:
            lines.append(f"   💵 Te sobran ${mejor['sobra']:.0f}")
    
    return "\n".join(lines)


async def handle_budget_search(
    wa_id: str,
    productos: Union[str, List[str]],
    presupuesto: int,
    personas: int,
    phone_number_id: str = None
):
    """
    Handler principal para búsqueda con presupuesto.
    Acepta un producto (str) o múltiples (list).
    """
    # Normalizar a lista
    if isinstance(productos, str):
        productos = [productos]
    
    print(f"[MENU-BUDGET] Usuario {wa_id[:6]}***: {productos} para {personas} con ${presupuesto}")
    
    resultados = await search_menu_with_budget(productos, presupuesto, personas)
    
    response = format_budget_response_multiple(resultados, productos, presupuesto, personas)
    
    await send_message(wa_id, response, phone_number_id)
    
    print(f"[MENU-BUDGET] ✅ Respuesta enviada a {wa_id[:6]}***")