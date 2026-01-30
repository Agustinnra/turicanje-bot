"""
Handler de Menú con Presupuesto.
Permite buscar productos con filtro de presupuesto y personas.
Agrupa resultados por NEGOCIO - solo muestra lugares con TODOS los productos.
Prioriza: cashback > mejor precio > menor sobra
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
    """Genera variaciones de búsqueda para un producto."""
    producto_lower = producto.lower().strip()
    variaciones = [producto_lower]
    
    if producto_lower.endswith('s') and len(producto_lower) > 3:
        variaciones.append(producto_lower[:-1])
    if producto_lower.endswith('es') and len(producto_lower) > 4:
        variaciones.append(producto_lower[:-2])
    if not producto_lower.endswith('s'):
        variaciones.append(producto_lower + 's')
    
    sinonimos = {
        'chela': ['cerveza', 'cervezas'],
        'chelas': ['cerveza', 'cervezas'],
        'refresco': ['refresco', 'refrescos', 'soda', 'sodas'],
        'soda': ['refresco', 'refrescos'],
        'hamburguesa': ['hamburguesa', 'hamburguesas', 'burger'],
        'burger': ['hamburguesa', 'hamburguesas'],
        'papas': ['papas', 'papa', 'papas fritas'],
        'agua': ['agua', 'aguas'],
        'taco': ['taco', 'tacos'],
        'tacos': ['taco', 'tacos'],
        'cafe': ['café', 'cafés', 'coffee'],
        'café': ['café', 'cafés', 'coffee'],
        'pizza': ['pizza', 'pizzas'],
        'torta': ['torta', 'tortas'],
        'cerveza': ['cerveza', 'cervezas', 'chela', 'chelas'],
    }
    
    if producto_lower in sinonimos:
        variaciones.extend(sinonimos[producto_lower])
    
    return list(dict.fromkeys(variaciones))


async def search_menu_by_negocio(
    productos: List[str], 
    presupuesto: int, 
    personas: int = 1
) -> Dict[str, Dict[str, Any]]:
    """
    Busca múltiples productos y agrupa por negocio.
    Incluye info de cashback y prioridad para ordenamiento.
    """
    try:
        pool = pool_getter()
        if not pool:
            print("[MENU-BUDGET] ❌ No hay conexión a BD")
            return {}
        
        negocios_data = {}
        
        for producto in productos:
            variaciones = normalizar_producto(producto)
            print(f"[MENU-BUDGET] Buscando '{producto}' con variaciones: {variaciones}")
            
            conditions = " OR ".join(["m.nombre ILIKE %s" for _ in variaciones])
            patterns = [f"%{v}%" for v in variaciones]
            
            sql = f"""
            SELECT 
                m.id, m.nombre, m.precio, m.categoria, m.place_id,
                p.name as negocio, p.address, p.cashback, p.priority,
                p.plan_activo, p.plan_fecha_vencimiento
            FROM menu_items m
            JOIN places p ON m.place_id = p.id
            WHERE m.disponible = true
              AND p.is_active = true
              AND m.precio <= %s
              AND ({conditions})
            ORDER BY 
                CASE WHEN p.cashback = true THEN 0 ELSE 1 END ASC,
                CASE WHEN p.plan_fecha_vencimiento >= CURRENT_DATE THEN 0 ELSE 1 END ASC,
                p.priority DESC NULLS LAST,
                m.precio ASC
            LIMIT 30;
            """
            
            params = [presupuesto] + patterns
            
            with pool.connection() as conn:
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    cur.execute(sql, tuple(params))
                    rows = cur.fetchall()
            
            for row in rows:
                place_id = row['place_id']
                
                if place_id not in negocios_data:
                    negocios_data[place_id] = {
                        'negocio': row['negocio'],
                        'address': row['address'],
                        'cashback': row['cashback'],
                        'priority': row['priority'] or 0,
                        'plan_activo': row['plan_activo'],
                        'productos': {}
                    }
                
                if producto not in negocios_data[place_id]['productos']:
                    negocios_data[place_id]['productos'][producto] = []
                
                precio = float(row['precio'])
                negocios_data[place_id]['productos'][producto].append({
                    'id': row['id'],
                    'nombre': row['nombre'],
                    'precio': precio,
                    'categoria': row['categoria']
                })
        
        return negocios_data
        
    except Exception as e:
        print(f"[MENU-BUDGET] ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {}


def calcular_combinacion(negocio_data: Dict, productos: List[str], presupuesto: int, personas: int) -> Dict:
    """
    Calcula la mejor combinación de productos en UN negocio.
    Divide el presupuesto equitativamente entre productos.
    """
    productos_disponibles = negocio_data['productos']
    num_productos = len(productos)
    presupuesto_por_tipo = presupuesto // num_productos if num_productos > 0 else presupuesto
    
    combinacion = []
    total_gasto = 0
    productos_encontrados = 0
    
    for producto in productos:
        if producto in productos_disponibles and productos_disponibles[producto]:
            # Tomar el más barato de este producto
            item = productos_disponibles[producto][0]
            cantidad = presupuesto_por_tipo // int(item['precio'])
            if cantidad > 0:
                gasto = item['precio'] * cantidad
                total_gasto += gasto
                productos_encontrados += 1
                combinacion.append({
                    'producto': producto,
                    'nombre': item['nombre'],
                    'precio': item['precio'],
                    'cantidad': int(cantidad),
                    'gasto': gasto
                })
    
    return {
        'combinacion': combinacion,
        'total_gasto': total_gasto,
        'sobra': presupuesto - total_gasto,
        'productos_encontrados': productos_encontrados,
        'productos_pedidos': num_productos,
        'tiene_todo': productos_encontrados == num_productos
    }


def format_budget_response_by_negocio(
    negocios_data: Dict[str, Dict],
    productos: List[str],
    presupuesto: int,
    personas: int
) -> str:
    """Formatea respuesta agrupada por negocio. Solo muestra los que tienen TODO."""
    
    if not negocios_data:
        productos_str = " y ".join(productos)
        return f"""😕 No encontré *{productos_str}* dentro de tu presupuesto de ${presupuesto:,} para {personas} personas.

💡 *Sugerencias:*
- Intenta con un presupuesto mayor
- Busca otros productos"""
    
    # Calcular combinaciones para cada negocio
    resultados = []
    for place_id, data in negocios_data.items():
        combo = calcular_combinacion(data, productos, presupuesto, personas)
        if combo['combinacion']:
            resultados.append({
                'place_id': place_id,
                'negocio': data['negocio'],
                'cashback': data['cashback'],
                'priority': data['priority'],
                **combo
            })
    
    # Filtrar SOLO los que tienen TODOS los productos
    resultados_completos = [r for r in resultados if r['tiene_todo']]
    
    if not resultados_completos:
        productos_str = " y ".join(productos)
        return f"""😕 No encontré un lugar que tenga *{productos_str}* dentro de tu presupuesto de ${presupuesto:,}.

💡 *Sugerencias:*
- Intenta con un presupuesto mayor
- Busca menos productos a la vez
- Escribe cada producto por separado"""
    
    # Ordenar por: cashback primero, luego prioridad, luego menor sobra
    resultados_completos.sort(key=lambda x: (
        0 if x['cashback'] else 1,  # Cashback primero
        -x['priority'],              # Mayor prioridad
        x['sobra']                   # Menor sobra (mejor aprovechamiento)
    ))
    
    lines = [f"🍽️ *Opciones para {personas} personas con ${presupuesto:,}*\n"]
    
    # Mostrar top 3 negocios que tienen todo
    for i, r in enumerate(resultados_completos[:3]):
        cashback_badge = " 💰" if r['cashback'] else ""
        emoji = "📍" if i == 0 else "📌"
        
        lines.append(f"{emoji} *{r['negocio']}*{cashback_badge}")
        
        for item in r['combinacion']:
            lines.append(f"   • {item['cantidad']}x {item['nombre']} (${item['gasto']:.0f})")
        
        if r['sobra'] > 0:
            lines.append(f"   💵 Total: ${r['total_gasto']:.0f} | Sobran ${r['sobra']:.0f}")
        else:
            lines.append(f"   💵 Total: ${r['total_gasto']:.0f} | ¡Exacto!")
        lines.append("")
    
    # Recomendación final
    mejor = resultados_completos[0]
    lines.append(f"✅ *Recomendación:* {mejor['negocio']}")
    if mejor['cashback']:
        lines.append("   💰 ¡Acumulas puntos con tu compra!")
    
    return "\n".join(lines)


async def handle_budget_search(
    wa_id: str,
    productos: Union[str, List[str]],
    presupuesto: int,
    personas: int,
    phone_number_id: str = None
):
    """Handler principal para búsqueda con presupuesto."""
    
    if isinstance(productos, str):
        productos = [productos]
    
    print(f"[MENU-BUDGET] Usuario {wa_id[:6]}***: {productos} para {personas} con ${presupuesto}")
    
    negocios_data = await search_menu_by_negocio(productos, presupuesto, personas)
    
    response = format_budget_response_by_negocio(negocios_data, productos, presupuesto, personas)
    
    await send_message(wa_id, response, phone_number_id)
    
    print(f"[MENU-BUDGET] ✅ Respuesta enviada a {wa_id[:6]}***")