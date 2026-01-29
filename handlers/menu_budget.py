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
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Busca productos en menu_items que quepan en el presupuesto.
    Incluye búsqueda por plurales, sinónimos y categorías.
    """
    try:
        pool = pool_getter()
        if not pool:
            print("[MENU-BUDGET] ❌ No hay conexión a BD")
            return []
        
        presupuesto_por_persona = presupuesto / personas if personas > 0 else presupuesto
        
        # ===== NORMALIZACIÓN DE BÚSQUEDA =====
        producto_lower = producto.lower().strip()
        
        # Generar variaciones (singular/plural)
        variaciones = [producto_lower]
        
        # Si termina en 's', agregar sin 's' (plural → singular)
        if producto_lower.endswith('s') and len(producto_lower) > 3:
            variaciones.append(producto_lower[:-1])
        # Si termina en 'es', agregar sin 'es'
        if producto_lower.endswith('es') and len(producto_lower) > 4:
            variaciones.append(producto_lower[:-2])
        # Agregar con 's' (singular → plural)
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
        
        # Eliminar duplicados manteniendo orden
        variaciones = list(dict.fromkeys(variaciones))
        
        print(f"[MENU-BUDGET] Variaciones de búsqueda: {variaciones}")
        
        # ===== BÚSQUEDA EN BD =====
        # Construir condiciones OR para cada variación
        conditions = " OR ".join(["m.nombre ILIKE %s" for _ in variaciones])
        patterns = [f"%{v}%" for v in variaciones]
        
        sql = f"""
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
          AND m.precio <= %s
          AND ({conditions})
        ORDER BY 
            CASE WHEN p.cashback = true THEN 0 ELSE 1 END ASC,
            m.precio ASC
        LIMIT %s;
        """
        
        # Parámetros: presupuesto, patrones de búsqueda, limit
        params = [presupuesto] + patterns + [limit]
        
        print(f"[MENU-BUDGET] Buscando con presupuesto máximo ${presupuesto}")
        
        with pool.connection() as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        
        # ===== SI NO HAY RESULTADOS, BUSCAR POR CATEGORÍA =====
        if not rows:
            print(f"[MENU-BUDGET] No encontró por nombre, buscando por categoría...")
            
            # Mapeo de productos a categorías
            categoria_map = {
                'cerveza': 'bebidas', 'cervezas': 'bebidas', 'chela': 'bebidas', 'chelas': 'bebidas',
                'refresco': 'bebidas', 'refrescos': 'bebidas', 'agua': 'bebidas',
                'hamburguesa': 'hamburguesas', 'hamburguesas': 'hamburguesas', 'burger': 'hamburguesas',
                'papas': 'complementos', 'papa': 'complementos',
                'taco': 'tacos', 'tacos': 'tacos',
                'postre': 'postres', 'postres': 'postres',
                'cafe': 'bebidas', 'café': 'bebidas',
            }
            
            categoria = categoria_map.get(producto_lower)
            
            if categoria:
                sql_cat = """
                SELECT 
                    m.id, m.nombre, m.precio, m.categoria, m.place_id,
                    p.name as negocio, p.address, p.cashback
                FROM menu_items m
                JOIN places p ON m.place_id = p.id
                WHERE m.disponible = true
                  AND m.precio <= %s
                  AND m.categoria = %s
                ORDER BY 
                    CASE WHEN p.cashback = true THEN 0 ELSE 1 END ASC,
                    m.precio ASC
                LIMIT %s;
                """
                
                with pool.connection() as conn:
                    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                        cur.execute(sql_cat, (presupuesto, categoria, limit))
                        rows = cur.fetchall()
                
                print(f"[MENU-BUDGET] Encontrados {len(rows)} por categoría '{categoria}'")
        
        # ===== PROCESAR RESULTADOS =====
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
        import traceback
        traceback.print_exc()
        return []


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