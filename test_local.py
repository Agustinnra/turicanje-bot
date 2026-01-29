# test_local.py
import requests
import json

# URL de tu bot local
LOCAL_URL = "http://localhost:8000/webhook"

# Simular mensaje de WhatsApp
def test_text_message(phone_from, text, phone_number_id="816732738189248"):
    """Simula un mensaje de texto de WhatsApp"""
    payload = {
        "entry": [{
            "changes": [{
                "value": {
                    "metadata": {
                        "phone_number_id": phone_number_id
                    },
                    "messages": [{
                        "from": phone_from,
                        "type": "text",
                        "text": {
                            "body": text
                        }
                    }]
                }
            }]
        }]
    }
    
    response = requests.post(LOCAL_URL, json=payload)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    return response

def test_location_message(phone_from, lat, lng, phone_number_id="816732738189248"):
    """Simula un mensaje de ubicación de WhatsApp"""
    payload = {
        "entry": [{
            "changes": [{
                "value": {
                    "metadata": {
                        "phone_number_id": phone_number_id
                    },
                    "messages": [{
                        "from": phone_from,
                        "type": "location",
                        "location": {
                            "latitude": lat,
                            "longitude": lng
                        }
                    }]
                }
            }]
        }]
    }
    
    response = requests.post(LOCAL_URL, json=payload)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    return response

# Ejemplos de uso:
if __name__ == "__main__":
    print("🧪 Testing Bot Local\n")
    
    # Test 1: Saludo
    print("1️⃣ Test: Saludo")
    test_text_message("5215512345678", "Hola")
    print("\n" + "="*50 + "\n")
    
    # Test 2: Búsqueda
    print("2️⃣ Test: Búsqueda de pizza")
    test_text_message("5215512345678", "quiero pizza")
    print("\n" + "="*50 + "\n")
    
    # Test 3: Ubicación
    print("3️⃣ Test: Enviar ubicación")
    test_location_message("5215512345678", 19.4326, -99.1332)  # CDMX
    print("\n" + "="*50 + "\n")
    
    # Test 4: Selección por número
    print("4️⃣ Test: Seleccionar opción 1")
    test_text_message("5215512345678", "1")