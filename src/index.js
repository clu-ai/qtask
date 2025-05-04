import Redis from 'ioredis';
import config from './config.js'; // Importa el archivo config.js
import Logging from './core/Logging.js'; // Importa la clase Logging

// --- Configuración del Logger ---
const log = new Logging({
    serviceName: 'RedisStreamExample',
    minLevel: 'info', // Muestra info, warn, error. Cambia a 'debug' para más detalle.
    useColors: true,
    timestampFormat: 'iso' // O 'local' o 'none'
});

// --- Configuración de Redis (Ajustada) ---
const redis = new Redis({
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    username: config.REDIS_USERNAME,
    password: config.REDIS_PASSWORD,
    // Manejo de reintentos de conexión
    retryStrategy(times) {
        const delay = Math.min(times * 100, 3000); // Incremento más suave, máximo 3 segundos
        log.warn(`Redis: Reintentando conexión (Intento ${times}). Próximo intento en ${delay}ms`);
        return delay;
    },
    // Opcional: Limita reintentos para comandos individuales si fallan
    maxRetriesPerRequest: 3
    // ELIMINADO reconnectOnError - no es necesario aquí
});

// --- Manejadores de Eventos de Conexión Redis ---
redis.on('connect', () => {
    log.info('Redis: Conectado exitosamente.');
});

redis.on('ready', () => {
    log.info('Redis: Cliente listo para ejecutar comandos.');
});

redis.on('error', (error) => {
    // Este handler es para errores generales del cliente o conexión subyacente,
    // no para errores de comando específicos (como BUSYGROUP) que se manejan con try/catch.
    log.error('Redis: Ocurrió un error general del cliente/conexión:', error.message);
    // Podríamos añadir más detalles si es necesario: log.error('...', error);
});

redis.on('close', () => {
    log.warn('Redis: Conexión cerrada.');
});

redis.on('reconnecting', (/* podrías recibir el delay aquí en versiones más nuevas de ioredis */) => {
    log.info('Redis: Reconectando...');
});

redis.on('end', () => {
    log.warn('Redis: Conexión finalizada (no se reintentará más).');
});


// Definir TOPIC y GROUP
const TOPIC = 'WHATSAPP';
const GROUP = 'fazpi-whatsapp';

// --- Funciones con Logging Integrado ---

async function createConsumerGroup() {
    try {
        // Intenta crear el grupo. MKSTREAM lo crea solo si el stream no existe.
        const groupExists = await redis.xgroup('CREATE', TOPIC, GROUP, '0', 'MKSTREAM');
        log.info(`Grupo de consumidores '${GROUP}' creado en stream '${TOPIC}'. Respuesta: ${groupExists}`);
    } catch (error) {
        // Este es el lugar PRINCIPAL para manejar el error BUSYGROUP
        if (error.message.includes('BUSYGROUP Consumer Group name already exists')) {
            log.info(`El grupo de consumidores '${GROUP}' ya existe en stream '${TOPIC}'.`);
        } else {
            // Loguear cualquier OTRO error que ocurra al intentar crear el grupo
            log.error(`Error al intentar crear/verificar grupo '${GROUP}' en stream '${TOPIC}':`, error);
        }
    }
}

async function publishMessage(message) {
    try {
        const messageId = await redis.xadd(TOPIC, '*', 'message', message);
        log.info(`Mensaje publicado en '${TOPIC}' con ID: ${messageId}`);
        log.debug(`Contenido del mensaje publicado: ${message}`);
    } catch (error) {
        log.error(`Error al publicar mensaje en '${TOPIC}':`, error);
    }
}

async function readMessages() {
    try {
        log.debug(`readMessages: Intentando XREAD BLOCK 5000 STREAMS ${TOPIC} $`);
        const result = await redis.xread('BLOCK', 5000, 'STREAMS', TOPIC, '$');

        if (result) {
            const [streamName, messages] = result[0];
            log.debug(`readMessages: XREAD retornó ${messages.length} mensaje(s) de ${streamName}`);

            if (messages.length > 0) {
                messages.forEach(([id, fields]) => {
                    const message = fields[1];
                    log.info(`Mensaje recibido (readMessages) de '${streamName}': ID ${id}, Contenido: ${message}`);
                });
            }
        } else {
            log.debug('readMessages: BLOCK timeout, no llegaron mensajes nuevos.');
        }

    } catch (error) {
        // Manejar error si el stream no existe para XREAD $
        if (error.message.includes('NOGROUP No such key') || error.message.includes('no such key')) {
             // Nota: XREAD $ puede fallar si el stream se borra. XADD lo recrea.
            log.warn(`readMessages: Stream '${TOPIC}' no encontrado (puede ser temporal).`);
        } else {
            log.error('readMessages: Error al leer los mensajes:', error);
        }
    }
}

async function consumeMessages() {
    const consumerId = 'consumer1';
    try {
        log.debug(`consumeMessages: Esperando mensajes del grupo '${GROUP}' para consumidor '${consumerId}'...`);
        const result = await redis.xreadgroup('GROUP', GROUP, consumerId, 'BLOCK', 0, 'STREAMS', TOPIC, '>');

        if (!result) {
            log.warn(`consumeMessages: XREADGROUP para '${GROUP}' retornó null inesperadamente.`);
            return; // Salir de esta iteración si no hay resultado
        }

        const [streamName, messages] = result[0];

        if (messages.length === 0) {
            log.warn(`consumeMessages: XREADGROUP para '${GROUP}' retornó 0 mensajes (inesperado con BLOCK 0).`);
        } else {
             log.debug(`consumeMessages: Recibidos ${messages.length} mensaje(s) para el grupo '${GROUP}'`);
            messages.forEach(async ([id, fields]) => {
                const message = fields[1];
                log.info(`Grupo '${GROUP}' (Consumidor '${consumerId}') - Mensaje recibido: ID ${id}, Contenido: ${message}`);

                try {
                    await redis.xack(TOPIC, GROUP, id);
                    log.debug(`Mensaje ${id} confirmado (XACK) para grupo '${GROUP}'.`);
                } catch (ackError) {
                     log.error(`Error al confirmar (XACK) mensaje ${id} para grupo '${GROUP}':`, ackError);
                }
            });
        }
    } catch (error) {
        // Manejar error si el grupo o stream no existe al intentar consumir
        if (error.message.includes('NOGROUP No such key') || error.message.includes('no such key')) {
            log.error(`consumeMessages: Stream '${TOPIC}' o grupo '${GROUP}' no existe. Reintentando creación de grupo...`);
            // Intentar recrear el grupo y luego esperar un poco antes del siguiente ciclo
            await createConsumerGroup();
            await new Promise(resolve => setTimeout(resolve, 5000)); // Espera 5s
        } else {
            log.error(`Error al consumir mensajes del grupo '${GROUP}':`, error);
            // Esperar un poco en caso de otros errores antes de reintentar
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }
}

// Función para verificar mensajes pendientes (opcional, si se usa)
async function checkPendingMessages() {
    try {
        const pending = await redis.xpending(TOPIC, GROUP);
        if (Array.isArray(pending) && pending[0] > 0) {
            log.warn(`Mensajes pendientes para grupo '${GROUP}': ${pending[0]}. MinID: ${pending[1]}, MaxID: ${pending[2]}`);
             if(pending[3] && pending[3].length > 0) {
                const consumerDetails = pending[3].map(([name, count]) => `${name}: ${count}`).join(', ');
                log.warn(`Detalle de pendientes por consumidor: ${consumerDetails}`);
             }
        } else if (Array.isArray(pending)) {
            log.info(`No hay mensajes pendientes para el grupo '${GROUP}'.`);
        } else {
             log.warn(`Respuesta inesperada de XPENDING para grupo '${GROUP}':`, pending);
        }
    } catch (error) {
        if (error.message.includes('NOGROUP No such key') || error.message.includes('no such key')) {
            log.warn(`checkPendingMessages: Stream '${TOPIC}' o grupo '${GROUP}' no existe.`);
        } else {
            log.error(`Error al verificar mensajes pendientes para grupo '${GROUP}':`, error);
        }
    }
}

// --- INICIO DE EJECUCIÓN ---

async function main() {
    log.info("--- Iniciando aplicación Redis Stream Example ---");

    // Crear/Verificar el grupo de consumidores al inicio
    await createConsumerGroup();

    // Publicar un mensaje cada 5 segundos
    setInterval(() => {
        const message = `Mensaje de prueba - ${new Date().toISOString()}`;
        log.debug(`Planificando publicación de mensaje: ${message}`);
        publishMessage(message);
    }, 5000);

    // Leer mensajes con XREAD $ (observador) cada 10 segundos
    setInterval(() => {
        readMessages();
    }, 10000);

    // Consumir mensajes con el grupo de consumidores en un bucle robusto
    (async () => {
        log.info("Iniciando bucle de consumo de mensajes...");
        while (true) {
            await consumeMessages();
            // No es necesario delay aquí por el BLOCK 0 de XREADGROUP
        }
    })().catch(err => {
        log.error("Error fatal en el bucle principal de consumo:", err);
        process.exit(1);
    });

    // Verificar mensajes pendientes cada 30 segundos (opcional, descomentar si se necesita)
    /*
    setInterval(() => {
        checkPendingMessages();
    }, 30000); // Intervalo más largo para no sobrecargar
    */

    log.info("--- Inicialización completa, escuchando eventos y mensajes ---");

}

// Ejecutar la función principal
main().catch(err => {
    log.error("Error crítico al iniciar la aplicación:", err);
    process.exit(1);
});