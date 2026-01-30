"""
Handler de Menú con Presupuesto.
Búsqueda en capas: exacto > amplio inteligente > solo categoría
"""
from typing import Optional, List, Dict, Any, Union
import psycopg.rows

pool_getter = None
send_message = None


def init(get_pool_func, send_msg_func):
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
    
    palabras = producto_lower.split()
    if len(palabras) > 1:
        sin_prep = ' '.join([p for p in palabras if p not in ['de', 'con', 'al', 'la', 'el']])
        if sin_prep != producto_lower:
            variaciones.append(sin_prep)
            if sin_prep.endswith('s'):
                variaciones.append(sin_prep[:-1])
            else:
                variaciones.append(sin_prep + 's')
    
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


def obtener_palabra_especifica(producto: str) -> str:
    """Detecta si el producto tiene una palabra específica (pastor, bistec, etc.)"""
    palabras_especificas = [
        'pastor', 'bistec', 'pollo', 'res', 'carnitas', 'suadero', 
        'chorizo', 'sirloin', 'arrachera', 'costilla', 'cecina',
        'cochinita', 'barbacoa', 'campechano', 'hawaiana', 'mexicana'
    ]
    producto_lower = producto.lower()
    for palabra in palabras_especificas:
        if palabra in producto_lower:
            return palabra
    return None


def buscar_producto_en_db(pool, producto: str, presupuesto: int, modo: str = 'exacto') -> List[Dict]:
    """
    Busca producto en la BD.
    modo='exacto': busca el término completo
    modo='amplio': busca palabra base + palabra específica (si existe)
    modo='solo_base': busca solo la palabra base (último recurso)
    """
    producto_lower = producto.lower().strip()
    palabras = producto_lower.split()
    palabra_especifica = obtener_palabra_especifica(producto)
    
    # Obtener palabra base (primera palabra sin preposiciones)
    palabra_base = palabras[0] if palabras else producto_lower
    if palabra_base.endswith('s'):
        palabra_base_singular = palabra_base[:-1]
    else:
        palabra_base_singular = palabra_base
    
    if modo == 'exacto':
        # Búsqueda exacta: el término completo o sin preposiciones
        variaciones = [producto_lower]
        if producto_lower.endswith('s'):
            variaciones.append(producto_lower[:-1])
        else:
            variaciones.append(producto_lower + 's')
        
        sin_prep = ' '.join([p for p in palabras if p not in ['de', 'con', 'al', 'la', 'el']])
        if sin_prep != producto_lower:
            variaciones.append(sin_prep)
            if sin_prep.endswith('s'):
                variaciones.append(sin_prep[:-1])
            else:
                variaciones.append(sin_prep + 's')
        
        conditions = " OR ".join(["m.nombre ILIKE %s" for _ in variaciones])
        params = [presupuesto] + [f"%{v}%" for v in variaciones]
        extra_condition = ""
        
    elif modo == 'amplio' and palabra_especifica:
        # Búsqueda amplia CON palabra específica: debe contener AMBAS
        variaciones_base = [palabra_base, palabra_base_singular, palabra_base + 's']
        variaciones_base = list(dict.fromkeys(variaciones_base))
        
        conditions = " OR ".join(["m.nombre ILIKE %s" for _ in variaciones_base])
        params = [presupuesto] + [f"%{v}%" for v in variaciones_base] + [f"%{palabra_especifica}%"]
        extra_condition = " AND m.nombre ILIKE %s"
        
    else:
        # Solo palabra base (sin requerir palabra específica)
        variaciones = normalizar_producto(palabra_base)
        conditions = " OR ".join(["m.nombre ILIKE %s" for _ in variaciones])
        params = [presupuesto] + [f"%{v}%" for v in variaciones]
        extra_condition = ""
    
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
      AND ({conditions}){extra_condition}
    ORDER BY 
        CASE WHEN p.cashback = true THEN 0 ELSE 1 END ASC,
        CASE WHEN p.plan_activo = true AND p.plan_fecha_vencimiento >= CURRENT_DATE THEN 0 ELSE 1 END ASC,
        p.priority DESC NULLS LAST,
        m.precio ASC
    LIMIT 30;
    """
    
    with pool.connection() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()


async def search_menu_by_negocio(
    productos: List[str], 
    presupuesto: int, 
    personas: int = 1
) -> tuple:
    """
    Busca múltiples productos y agrupa por negocio.
    Retorna (negocios_data, avisos)
    """
    try:
        pool = pool_getter()
        if not pool:
            print("[MENU-BUDGET] ❌ No hay conexión a BD")
            return {}, []
        
        negocios_data = {}
        avisos = []  # Para guardar avisos de búsqueda amplia
        
        for producto in productos:
            print(f"[MENU-BUDGET] Buscando '{producto}'...")
            palabra_especifica = obtener_palabra_especifica(producto)
            
            # CAPA 1: Búsqueda exacta
            rows = buscar_producto_en_db(pool, producto, presupuesto, modo='exacto')
            modo_usado = 'exacto'
            
            # CAPA 2: Búsqueda amplia con palabra específica
            if not rows and palabra_especifica:
                print(f"[MENU-BUDGET] No exacto, buscando amplio con '{palabra_especifica}'...")
                rows = buscar_producto_en_db(pool, producto, presupuesto, modo='amplio')
                modo_usado = 'amplio_especifico'
            
            # CAPA 3: Solo palabra base (último recurso, sin palabra específica)
            if not rows:
                palabra_base = producto.lower().split()[0] if producto.split() else producto
                print(f"[MENU-BUDGET] No encontrado, buscando solo '{palabra_base}'...")
                rows = buscar_producto_en_db(pool, producto, presupuesto, modo='solo_base')
                modo_usado = 'solo_base'
                if rows:
                    avisos.append(f"No encontré \"{producto}\" exacto, mostrando opciones de \"{palabra_base}\"")
            
            print(f"[MENU-BUDGET] Encontrados {len(rows)} resultados para '{producto}' (modo: {modo_usado})")
            
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
        
        return negocios_data, avisos
        
    except Exception as e:
        print(f"[MENU-BUDGET] ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {}, []


def calcular_combinacion(negocio_data: Dict, productos: List[str], presupuesto: int, personas: int) -> Dict:
    productos_disponibles = negocio_data['productos']
    num_productos = len(productos)
    presupuesto_por_tipo = presupuesto // num_productos if num_productos > 0 else presupuesto
    
    combinacion = []
    total_gasto = 0
    productos_encontrados = 0
    
    for producto in productos:
        if producto in productos_disponibles and productos_disponibles[producto]:
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
    personas: int,
    avisos: List[str] = None
) -> str:
    
    if not negocios_data:
        productos_str = " y ".join(productos)
        return f"""😕 No encontré *{productos_str}* dentro de tu presupuesto de ${presupuesto:,} para {personas} personas.

💡 *Sugerencias:*
- Intenta con un presupuesto mayor
- Busca otros productos"""
    
    resultados = []
    for place_id, data in negocios_data.items():
        combo = calcular_combinacion(data, productos, presupuesto, personas)
        if combo['combinacion']:
            resultados.append({
                'place_id': place_id,
                'negocio': data['negocio'],
                'cashback': data['cashback'],
                'priority': data['priority'],
                'plan_activo': data['plan_activo'],
                'productos_disponibles': data['productos'],
                **combo
            })
    
    resultados_completos = [r for r in resultados if r['tiene_todo']]
    
    if resultados_completos:
        resultados_completos.sort(key=lambda x: (
            0 if x['cashback'] else 1,
            0 if x['plan_activo'] else 1,
            -x['priority'],
            x['sobra']
        ))
        
        lines = [f"🍽️ *Opciones para {personas} personas con ${presupuesto:,}*\n"]
        
        # Mostrar avisos si los hay
        if avisos:
            for aviso in avisos:
                lines.append(f"⚠️ {aviso}")
            lines.append("")
        
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
        
        mejor = resultados_completos[0]
        lines.append(f"✅ *Recomendación:* {mejor['negocio']}")
        if mejor['cashback']:
            lines.append("   💰 ¡Acumulas puntos con tu compra!")
        
        return "\n".join(lines)
    
    return format_opcion_separada(negocios_data, productos, presupuesto, personas, avisos)


def format_opcion_separada(
    negocios_data: Dict[str, Dict],
    productos: List[str],
    presupuesto: int,
    personas: int,
    avisos: List[str] = None
) -> str:
    num_productos = len(productos)
    presupuesto_por_producto = presupuesto // num_productos
    
    lines = [f"🍽️ *Búsqueda para {personas} personas con ${presupuesto:,}*\n"]
    
    if avisos:
        for aviso in avisos:
            lines.append(f"⚠️ {aviso}")
        lines.append("")
    
    lines.append("⚠️ *No hay un lugar con todo lo que buscas.*")
    lines.append("Pero puedes comprar por separado:\n")
    
    sugerencias = []
    total_gasto = 0
    productos_no_encontrados = []
    
    for producto in productos:
        mejor_opcion = None
        mejor_score = float('inf')
        
        for place_id, data in negocios_data.items():
            if producto in data['productos'] and data['productos'][producto]:
                item = data['productos'][producto][0]
                score = item['precio']
                if data['cashback']:
                    score -= 1000
                if data['plan_activo']:
                    score -= 500
                
                if score < mejor_score:
                    mejor_score = score
                    mejor_opcion = {
                        'negocio': data['negocio'],
                        'cashback': data['cashback'],
                        'nombre': item['nombre'],
                        'precio': item['precio']
                    }
        
        if mejor_opcion:
            cantidad = max(1, presupuesto_por_producto // int(mejor_opcion['precio']))
            gasto = mejor_opcion['precio'] * cantidad
            total_gasto += gasto
            sugerencias.append({
                'producto': producto,
                'cantidad': cantidad,
                'gasto': gasto,
                **mejor_opcion
            })
        else:
            productos_no_encontrados.append(producto)
    
    if not sugerencias:
        productos_str = " y ".join(productos)
        return f"😕 No encontré opciones para *{productos_str}* dentro de tu presupuesto."
    
    for s in sugerencias:
        cashback_badge = " 💰" if s['cashback'] else ""
        lines.append(f"📍 *{s['producto'].upper()}* en {s['negocio']}{cashback_badge}")
        lines.append(f"   • {s['cantidad']}x {s['nombre']} = ${s['gasto']:.0f}")
        lines.append("")
    
    if productos_no_encontrados:
        lines.append(f"❌ No encontré: {', '.join(productos_no_encontrados)}")
        lines.append("")
    
    sobra = presupuesto - total_gasto
    lines.append(f"━━━━━━━━━━━━━━━")
    lines.append(f"💵 *Total:* ${total_gasto:.0f}")
    if sobra > 0:
        lines.append(f"💰 *Te sobran:* ${sobra:.0f}")
    
    return "\n".join(lines)


async def handle_budget_search(
    wa_id: str,
    productos: Union[str, List[str]],
    presupuesto: int,
    personas: int,
    phone_number_id: str = None
):
    if isinstance(productos, str):
        productos = [productos]
    
    if len(productos) > 4:
        productos = productos[:4]
    
    print(f"[MENU-BUDGET] Usuario {wa_id[:6]}***: {productos} para {personas} con ${presupuesto}")
    
    negocios_data, avisos = await search_menu_by_negocio(productos, presupuesto, personas)
    
    response = format_budget_response_by_negocio(negocios_data, productos, presupuesto, personas, avisos)
    
    await send_message(wa_id, response, phone_number_id)
    
    print(f"[MENU-BUDGET] ✅ Respuesta enviada a {wa_id[:6]}***")