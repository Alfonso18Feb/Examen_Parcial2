# publicador.py
import asyncio
import aiohttp
import pika
import xml.etree.ElementTree as ET

# Conexión y envío a RabbitMQ
def enviar_a_rabbitmq(mensaje: str):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='noticias', exchange_type='fanout')
    channel.basic_publish(exchange='noticias', routing_key='', body=mensaje.encode())
    connection.close()
    print("[X] Enviado a RabbitMQ:", mensaje)

# Obtener noticias desde un feed RSS
async def obtener_noticias_rss(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                xml_data = await response.text()
                root = ET.fromstring(xml_data)
                
                # Buscar los elementos <item> en el XML (que representan las noticias)
                noticias = []
                for item in root.findall('.//item'):
                    title = item.find('title').text
                    if title:
                        noticias.append(title)
                
                return noticias[:5]  # Obtener solo las primeras 5 noticias
            else:
                print(f"[!] Error {response.status} al acceder {url}")
    except Exception as e:
        print(f"[!] Excepción con {url}: {e}")
    return []

# Función principal
async def main():
    urls = [
        "http://rss.cnn.com/rss/edition.rss",  # RSS Feed de CNN
        "http://feeds.bbci.co.uk/news/rss.xml"  # RSS Feed de BBC
    ]
    
    async with aiohttp.ClientSession() as session:
        tareas = [obtener_noticias_rss(session, url) for url in urls]
        resultados = await asyncio.gather(*tareas)

    # Unificar y enviar las noticias (5 de cada fuente)
    for fuente in resultados:
        for noticia in fuente:
            enviar_a_rabbitmq(noticia)

# Ejecutar
asyncio.run(main())
