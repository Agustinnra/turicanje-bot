"""
Servicio de WhatsApp para envío de mensajes y medios.
"""
from typing import Optional
import httpx

# Configuración (se inicializa desde app.py)
config = {
    "send_via_whatsapp": True,
    "whatsapp_token": "",
    "phone_number_id": "",
    "environment": "production"
}

# Función para obtener config por phone_number_id
_get_environment_config = None


def init(send_via_whatsapp: bool, whatsapp_token: str, phone_number_id: str, 
         environment: str, get_env_config_func):
    """Inicializa la configuración del servicio."""
    global config, _get_environment_config
    config["send_via_whatsapp"] = send_via_whatsapp
    config["whatsapp_token"] = whatsapp_token
    config["phone_number_id"] = phone_number_id
    config["environment"] = environment
    _get_environment_config = get_env_config_func


def _get_config(phone_number_id: str = None) -> dict:
    """Obtiene la configuración según el phone_number_id."""
    if phone_number_id and _get_environment_config:
        return _get_environment_config(phone_number_id)
    else:
        return {
            "env": config["environment"].upper(),
            "phone_number_id": config["phone_number_id"],
            "whatsapp_token": config["whatsapp_token"],
            "prefix": f"[{config['environment'].upper()}]"
        }


async def send_whatsapp_message(to: str, message: str, phone_number_id: str = None):
    """Envía un mensaje de texto por WhatsApp."""
    env_config = _get_config(phone_number_id)
    
    if not config["send_via_whatsapp"]:
        print(f"\n{env_config['prefix']} [DRY-RUN] Mensaje a {to}:")
        print(f"{message}\n")
        return
    
    if not env_config["whatsapp_token"]:
        print(f"{env_config['prefix']} [ERROR] Falta token")
        return
    
    url = f"https://graph.facebook.com/v20.0/{env_config['phone_number_id']}/messages"
    headers = {"Authorization": f"Bearer {env_config['whatsapp_token']}"}
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
                print(f"{env_config['prefix']} [ERROR] WhatsApp API: {response.status_code} - {response.text}")
            else:
                print(f"{env_config['prefix']} [OK] Mensaje enviado a {to}")
    except Exception as e:
        print(f"{env_config['prefix']} [ERROR] Enviando mensaje: {e}")


async def send_whatsapp_image(to: str, image_url: str, caption: Optional[str] = None, phone_number_id: str = None):
    """Envía una imagen por WhatsApp."""
    env_config = _get_config(phone_number_id)
    
    if not config["send_via_whatsapp"]:
        print(f"\n{env_config['prefix']} [DRY-RUN] Imagen a {to}: {image_url}")
        if caption:
            print(f"Caption: {caption}")
        return
    
    if not env_config["whatsapp_token"]:
        print(f"{env_config['prefix']} [ERROR] Falta token de WhatsApp para imagen")
        return
    
    url = f"https://graph.facebook.com/v20.0/{env_config['phone_number_id']}/messages"
    headers = {"Authorization": f"Bearer {env_config['whatsapp_token']}"}
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
                print(f"{env_config['prefix']} [ERROR] WhatsApp Image API: {response.status_code} - {response.text}")
            else:
                print(f"{env_config['prefix']} [OK] Imagen enviada a {to}")
    except Exception as e:
        print(f"{env_config['prefix']} [ERROR] Enviando imagen: {e}")