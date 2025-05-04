import config from '../src/config.js';        // Importa la configuración
import QTask from '../src/index.js';             // Importa tu clase principal QTask
// Solo necesitas disconnectRedis por si falla la creación de QTask antes del stop
import { disconnect as disconnectRedis } from '../src/core/RedisConnection.js';

// --- Configuración del Ejemplo ---
const WHATSAPP_TOPIC_BASE = config.WHATSAPP_TOPIC_BASE || 'WHATSAPP'; // Nombre BASE del topic
const WHATSAPP_GROUP = config.WHATSAPP_GROUP || 'fazpi-whatsapp';   // Nombre del grupo
// ¡IMPORTANTE! Obtener el número total de particiones
const TOTAL_PARTITIONS = parseInt(config.TOTAL_PARTITIONS || '1', 10); // Tomar de config o default (ej: 4)

if (isNaN(TOTAL_PARTITIONS) || TOTAL_PARTITIONS <= 0) {
    console.error("Error: TOTAL_PARTITIONS debe ser un número entero positivo en la configuración.");
    process.exit(1);
}

// --- Handler para los mensajes (ahora recibe partitionIndex) ---
/**
 * Handler específico para mensajes del stream WHATSAPP.
 * @param {string} id - ID del mensaje de Redis.
 * @param {object} data - El objeto de datos del mensaje reconstruido.
 * @param {Logging} handlerLog - Instancia del logger.
 * @param {number} partitionIndex - El índice de la partición de donde viene el mensaje.
 */
async function handleWhatsappMessage(id, data, handlerLog, partitionIndex) {
    // Loguea incluyendo la partición
    handlerLog.info(`[Handler P:${partitionIndex}] Procesando mensaje WhatsApp ${id}:`, data);
    try {
        // Simula algún trabajo específico de la partición o del mensaje
        await new Promise(resolve => setTimeout(resolve, 50 + Math.random() * 100));

        // Lógica de ejemplo: Validar que el mensaje tenga un destinatario 'to'
        if (!data || !data.to) {
            throw new Error(`Mensaje ${id} en partición ${partitionIndex} no tiene destinatario 'to'.`);
        }

        // Simula error basado en la propiedad del mensaje
        if (data.simulateError === 'true' || data.simulateError === true) {
            throw new Error(`Simulación de error procesando mensaje ${id} en partición ${partitionIndex}`);
        }

        handlerLog.info(`[Handler P:${partitionIndex}] Mensaje WhatsApp ${id} procesado OK.`);
        // Recuerda: No necesitas llamar a ack() aquí, el ConsumerManager lo hace por ti si esta función retorna sin error.
    } catch (processingError) {
         handlerLog.error(`[Handler P:${partitionIndex}] Error procesando ${id}:`, processingError);
         // Relanzar el error es importante para que ConsumerManager sepa que falló y NO haga ACK.
         throw processingError;
    }
}

// --- Función Principal del Ejemplo ---
async function main() {
    let qtaskInstance = null;
    let publishInterval = null; // Handle para limpiar el intervalo

    try {
        // 1. Crear instancia de QTask con configuración (incluyendo TOTAL_PARTITIONS)
        qtaskInstance = new QTask({
            // Configuración Redis
            REDIS_HOST: config.REDIS_HOST,
            REDIS_PORT: config.REDIS_PORT,
            REDIS_USERNAME: config.REDIS_USERNAME,
            REDIS_PASSWORD: config.REDIS_PASSWORD,
            // Configuración QTask / Particionamiento
            TOTAL_PARTITIONS: TOTAL_PARTITIONS, // <-- Necesario para el Partitioner interno
            // Configuración Logger
            logLevel: process.env.NODE_ENV === 'development' ? 'debug' : 'info',
            logServiceName: `ExamplePart-${process.env.INSTANCE_ID || 0}` // ID de instancia (opcional)
        });

        // 2. Conectar
        await qtaskInstance.connect();

        // 3. Registrar el handler para el topic base
        // ConsumerManager usará las variables de entorno INSTANCE_ID e INSTANCE_COUNT
        // (o los defaults 0 y 1) para determinar qué particiones escuchar.
        // En una ejecución local simple, escuchará todas las particiones (0 a TOTAL_PARTITIONS-1).
        await qtaskInstance.register({
            topic: WHATSAPP_TOPIC_BASE, // <-- Pasa el nombre BASE
            group: WHATSAPP_GROUP,
            handler: handleWhatsappMessage    // <-- Pasa tu handler actualizado
            // No necesitamos pasar 'partitioning' explícitamente aquí
            // si confiamos en las variables de entorno o los defaults (0 de 1).
        });

        // 4. Iniciar publicación periódica con claves de partición variables
        qtaskInstance.log.info('[ExampleBasic] Iniciando publicación periódica...');
        let counter = 0;
        // Guardar el handle del intervalo para poder limpiarlo al salir
        publishInterval = setInterval(() => {
            counter++;
            // Simula diferentes destinatarios para usar como clave de partición
            const recipient = `57310${Math.floor(Math.random() * 10000000).toString().padStart(7, '0')}`;
            const message = {
                 to: recipient, // Usa el destinatario como base para la clave
                 message: `Mensaje ${counter} para ${recipient}`,
                 timestamp: new Date().toISOString(),
                 simulateError: (counter % 7 === 0).toString() // Simula error cada 7 mensajes
            };

            qtaskInstance.log.debug(`[ExampleBasic] Publicando para key ${recipient}:`, message);
            // Llama a publish con: topic, partitionKey, messageData
            qtaskInstance.publish(WHATSAPP_TOPIC_BASE, recipient, message);

        }, 5000); // Publica cada 5 segundos


        qtaskInstance.log.info("--- Ejemplo Básico Particionado inicializado ---");
        qtaskInstance.log.info(`Escuchando particiones asignadas para ${WHATSAPP_TOPIC_BASE}. Presiona Ctrl+C para salir.`);

    } catch (error) {
        console.error("Error fatal durante la inicialización del ejemplo:", error);
        if (qtaskInstance && typeof qtaskInstance.stop === 'function') {
            await qtaskInstance.stop().catch(e => console.error("Error durante stop:", e));
        }
        // Ya no llamamos a disconnectRedis aquí (manejado por stop o no necesario)
        process.exit(1);
    }

    // --- Manejo de cierre grácil ---
    const shutdown = async (signal) => {
        console.log(`\nRecibido ${signal}. Iniciando cierre grácil...`);
        // Limpiar el intervalo de publicación si está activo
        if (publishInterval) {
            clearInterval(publishInterval);
            console.log("Intervalo de publicación detenido.");
        }
        if (qtaskInstance && typeof qtaskInstance.stop === 'function') {
            await qtaskInstance.stop(); // Llama al stop de QTask (detiene consumers y desconecta redis)
        } else {
            console.log("[Shutdown] No hay instancia QTask activa que detener.");
             // Podríamos intentar desconectar aquí si RedisConnection estuviera importado y connect se llamó,
             // pero es menos probable que sea necesario si QTask no se instanció/conectó.
        }
        console.log('Cierre completado. Adiós.');
        process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
}

// --- Ejecutar el Ejemplo ---
main().catch(async (err) => { // async para el await
    console.error("Error crítico inesperado en la ejecución principal del ejemplo:", err);
    // Intenta detener QTask si existe, incluso si el error fue después de main()
    if (qtaskInstance && typeof qtaskInstance.stop === 'function') {
        await qtaskInstance.stop().catch(e => console.error("Error durante stop en catch final:", e));
    }
    process.exit(1);
});