// Ya no importa ioredis directamente
import config from './config.js';
import Logging from './core/Logging.js';
import ConsumerManager from './core/ConsumerManager.js';
import Publisher from './core/Publisher.js';
// Importa las funciones de conexión
import { connect as connectRedis, disconnect as disconnectRedis } from './core/RedisConnection.js';

// --- Configuración del Logger (sin cambios) ---
const log = new Logging({ serviceName: 'RedisApp', minLevel: 'debug', useColors: true, timestampFormat: 'iso' });

// --- Ya NO se crea la instancia de Redis aquí ---
// --- Ya NO se necesitan los listeners de eventos Redis aquí ---

// Definir TOPIC y GROUP
const WHATSAPP_TOPIC = 'WHATSAPP';
const WHATSAPP_GROUP = 'fazpi-whatsapp';

// --- Handlers (sin cambios) ---
async function handleWhatsappMessage(id, data, handlerLog) {
    // ... (lógica del handler sin cambios) ...
    handlerLog.info(`Procesando mensaje WhatsApp ${id}:`, data);
    try {
        if (!data || typeof data !== 'object') {
            throw new Error('Formato de datos inesperado para mensaje WhatsApp');
        }
        await new Promise(resolve => setTimeout(resolve, 50 + Math.random() * 50));
        if (data.simulateError === 'true' || data.simulateError === true) {
            throw new Error('Simulación de error de procesamiento en WhatsApp');
        }
        handlerLog.info(`Mensaje WhatsApp ${id} procesado OK.`);
    } catch (processingError) {
        handlerLog.error(`Error procesando mensaje WhatsApp ${id}:`, processingError);
        throw processingError;
    }
}

// --- Lógica Principal ---

async function main() {
    log.info("--- Iniciando aplicación ---");

    // 1. Conectar a Redis usando el módulo de conexión
    try {
        // Pasa los parámetros desde tu config.js
        await connectRedis({
            host: config.REDIS_HOST,
            port: config.REDIS_PORT,
            username: config.REDIS_USERNAME,
            password: config.REDIS_PASSWORD,
            // otras opciones si las tienes en config
        }, log);
        log.info("Conexión Redis establecida a través del módulo.");
    } catch (redisConnError) {
        log.error("Error crítico al conectar a Redis. Saliendo.", redisConnError);
        process.exit(1);
    }


    // 2. Crear instancias de Manager y Publisher (ahora solo necesitan log)
    const manager = new ConsumerManager({ log });
    const publisher = new Publisher({ log });

    // 3. Registrar los handlers/consumidores
    try {
        await manager.register({
            topic: WHATSAPP_TOPIC,
            group: WHATSAPP_GROUP,
            handler: handleWhatsappMessage,
        });
        // ... otros registros ...
    } catch (registerError) {
        // El manager ahora puede fallar aquí si no puede crear el grupo (ej. Redis desconectado)
        log.error("Error crítico al registrar consumidores. Saliendo.", registerError);
        await disconnectRedis(); // Intenta desconectar limpiamente
        process.exit(1);
    }


    // 4. Iniciar publicador periódico (sin cambios)
    let counter = 0;
    setInterval(() => {
        counter++;
        // --- CORRECCIÓN AQUÍ: Define el objeto completo ---
        const message = {
            type: 'whatsapp_event',
            timestamp: new Date().toISOString(),
            payload: `Evento ${counter}`,
            // Asegúrate que el valor sea string si así lo comparas en el handler
            simulateError: (counter % 5 === 0).toString()
        };
        // --- FIN CORRECCIÓN ---

        // Opcional: Loguea el objeto ANTES de enviarlo
        log.debug('[INDEX-DEBUG] Publicando objeto:', message);

        // Llama a publish con el objeto correcto
        publisher.publish(WHATSAPP_TOPIC, message);
    }, 7000); // Publica cada 7 segundos

    log.info("--- Aplicación inicializada. Gestor y Publisher activos. ---");

    // 5. Manejo de cierre grácil (ahora llama a disconnectRedis)
    const shutdown = async (signal) => {
        log.info(`Recibido ${signal}. Deteniendo procesos...`);
        await manager.stopAll();    // Detiene los consumidores
        await disconnectRedis();  // Desconecta Redis
        log.info('Procesos detenidos. Adiós.');
        process.exit(0);
    };
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
}

// Ejecutar main
main().catch(err => {
    log.error("Error crítico inesperado en main:", err);
    // Intenta desconectar Redis incluso si main falla
    disconnectRedis().finally(() => process.exit(1));
});

// Ya NO necesitas la función createConsumerGroupIfNotExists aquí fuera.
// Ya NO necesitas la función publishMessage aquí fuera.