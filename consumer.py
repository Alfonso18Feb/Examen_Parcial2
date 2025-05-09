# consumidor.py
import pika
import threading
import queue
from datetime import datetime

# Cola compartida que actuará como buffer
buffer = queue.Queue()

# Callback para recibir mensajes de RabbitMQ
def callback(ch, method, properties, body):
    print(f"[RECIBIDO] {body.decode()}")
    # Almacenar el mensaje en el buffer
    buffer.put(body.decode())

# Función que crea la noticia con el formato TÍTULO, FECHA y CONTENIDO
def procesar_noticias():
    while True:
        try:
            # Esperar a que haya un mensaje en el buffer
            mensaje = buffer.get(timeout=10)  # Espera de 10 segundos para terminar si no hay trabajo

            if mensaje is None:  # Si el mensaje es None, significa que debemos salir
                break
            
            # Procesar el mensaje y crear la noticia
            print("Procesando noticia...")
            fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Suponiendo que el mensaje es un diccionario con clave 'titulo' y 'contenido'
            try:
                noticia = eval(mensaje)  # Esto puede ser un JSON o un dict como cadena
                titulo = noticia.get('titulo', 'Sin título')
                contenido = noticia.get('contenido', 'Sin contenido')
                print(f"Noticia generada: \nTítulo: {titulo}\nFecha: {fecha}\nContenido: {contenido}\n")
            except Exception as e:
                print(f"Error al procesar el mensaje: {e}")
        
        except queue.Empty:
            continue  # Si el buffer está vacío, vuelve a verificar

# Función para consumir desde los servidores RabbitMQ
def consumir_desde_servidores():
    # Aquí puedes agregar varios servidores si los necesitas (localhost, Frankfurt, etc.)
    servidores = [
        'localhost',  # Para este caso, usar localhost en todas las terminales
    ]

    for servidor in servidores:
        # Conexión a cada servidor RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(servidor))
        channel = connection.channel()
        
        # Declarar el exchange 'noticias' como 'fanout'
        channel.exchange_declare(exchange='noticias', exchange_type='fanout')
        
        # Crear una cola exclusiva para este consumidor
        resultado = channel.queue_declare(queue='', exclusive=True)
        nombre_cola = resultado.method.queue
        
        # Vincular la cola al exchange 'noticias'
        channel.queue_bind(exchange='noticias', queue=nombre_cola)
        
        # Comenzar a consumir los mensajes
        channel.basic_consume(queue=nombre_cola, on_message_callback=callback, auto_ack=True)
        
        print(f"[*] Esperando mensajes en {servidor}. Pulsa CTRL+C para salir.")
        
        # Iniciar hilos dinámicos que procesen las noticias
        hilos_procesamiento = []
        while True:
            # Lanza un hilo de procesamiento cuando hay trabajo en el buffer
            if not buffer.empty():
                hilo = threading.Thread(target=procesar_noticias)
                hilo.start()
                hilos_procesamiento.append(hilo)
            
            # El consumidor sigue recibiendo mensajes
            channel.start_consuming()

            # Espera que todos los hilos terminen su trabajo
            for hilo in hilos_procesamiento:
                hilo.join()

# Ejecutar el consumidor
consumir_desde_servidores()
