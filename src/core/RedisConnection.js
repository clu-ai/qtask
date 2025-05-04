import Redis from 'ioredis';
import Logging from './Logging.js'; // Import Logging para el logger interno si es necesario

let redisClient = null; // Variable para almacenar la instancia única
let connectionLog = null; // Logger para los eventos de conexión

/**
 * Conecta a Redis y configura los listeners básicos. Debe llamarse una vez al inicio.
 * @param {object} options - Opciones de conexión.
 * @param {string} options.host - Host de Redis.
 * @param {number} options.port - Puerto de Redis.
 * @param {string} [options.username] - Nombre de usuario (si aplica).
 * @param {string} [options.password] - Contraseña de Redis.
 * @param {Logging} log - Instancia del logger principal.
 * @returns {Promise<Redis>} - Promesa que resuelve con el cliente conectado o rechaza si falla la conexión inicial.
 */
async function connect(options, log) {
    if (redisClient) {
        log.warn('[RedisConnection] Ya existe una conexión Redis.');
        return redisClient;
    }

    if (!options || !log) {
        throw new Error('[RedisConnection] Se requieren opciones de conexión y una instancia de logger.');
    }

    connectionLog = log; // Guarda el logger para usarlo en los eventos
    log.info('[RedisConnection] Creando y conectando instancia de Redis...');

    // Crear la instancia
    const client = new Redis({
        host: options.host,
        port: options.port,
        username: options.username, // ioredis maneja si es undefined
        password: options.password, // ioredis maneja si es undefined
        lazyConnect: true, // No conectar hasta que se llame a .connect() o se ejecute un comando
        retryStrategy(times) {
            const delay = Math.min(times * 100, 3000);
            connectionLog.warn(`[RedisConnection] Reintentando conexión (Intento ${times}). Próximo intento en ${delay}ms`);
            return delay;
        },
        maxRetriesPerRequest: options.maxRetriesPerRequest || 3,
        enableReadyCheck: true, // Importante para el evento 'ready'
         // Podrías añadir más opciones aquí si las necesitas (tls, db, etc.)
         // Opciones de Keepalive (pueden ayudar a detectar conexiones muertas)
         keepAlive: 1000 * 30, // Envía PING cada 30 segundos
         showFriendlyErrorStack: process.env.NODE_ENV !== 'production', // Más detalles en desarrollo
    });

    // Configurar listeners de eventos
    client.on('connect', () => connectionLog.info('[RedisConnection] Conectado exitosamente.'));
    client.on('ready', () => connectionLog.info('[RedisConnection] Cliente listo para ejecutar comandos.'));
    client.on('error', (error) => connectionLog.error('[RedisConnection] Error general:', error.message));
    client.on('close', () => connectionLog.warn('[RedisConnection] Conexión cerrada.'));
    client.on('reconnecting', () => connectionLog.info('[RedisConnection] Reconectando...'));
    client.on('end', () => {
        connectionLog.warn('[RedisConnection] Conexión finalizada (no se reintentará más).');
        redisClient = null; // Permite reconectar si es necesario después
    });

    try {
        // Intentar conectar explícitamente
        await client.connect();
        redisClient = client; // Almacena la instancia solo si conecta exitosamente
        connectionLog.info('[RedisConnection] Conexión establecida y verificada.');
        return redisClient;
    } catch (connectionError) {
        connectionLog.error('[RedisConnection] Falló la conexión inicial:', connectionError);
        // Asegurarse de limpiar listeners si la conexión inicial falla catastróficamente
        await client.quit().catch(() => {}); // Intenta cerrar si está parcialmente abierta
        throw connectionError; // Relanzar el error para que el inicio de la app falle
    }
}

/**
 * Obtiene la instancia del cliente Redis conectado.
 * @returns {Redis} - La instancia de ioredis.
 * @throws {Error} Si la conexión no ha sido establecida.
 */
function getClient() {
    if (!redisClient || !redisClient.status || redisClient.status === 'end') {
        // Podrías intentar reconectar aquí si `redisClient` es null pero hubo un intento previo
        // o simplemente lanzar el error. Por ahora, lanzamos error.
        throw new Error('[RedisConnection] Cliente Redis no está conectado o la conexión ha finalizado. Llama a connect() primero.');
    }
    return redisClient;
}

/**
 * Cierra la conexión Redis de forma grácil.
 * @returns {Promise<void>}
 */
async function disconnect() {
    if (redisClient) {
        connectionLog.info('[RedisConnection] Desconectando cliente Redis...');
        try {
            await redisClient.quit();
            connectionLog.info('[RedisConnection] Cliente Redis desconectado.');
            redisClient = null;
        } catch (error) {
            connectionLog.error('[RedisConnection] Error al desconectar cliente Redis:', error);
             // Forzar cierre si quit falla
             await redisClient.disconnect();
             redisClient = null;
        }
    } else {
        connectionLog.info('[RedisConnection] No hay cliente Redis activo para desconectar.');
    }
}

// Exporta las funciones para gestionar la conexión
export { connect, getClient, disconnect };