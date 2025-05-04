// Importa la clase para el logger personalizado
import Logging from './Logging.js';

// Importa la clase que gestiona los consumidores
import ConsumerManager from './ConsumerManager.js';

// Importa la clase que gestiona la publicación de mensajes
import Publisher from './Publisher.js';

// Importa las funciones para manejar la conexión centralizada de Redis
import { connect as connectRedis, disconnect as disconnectRedis } from './RedisConnection.js';

class QTask {
    /**
     * Crea la instancia principal de QTask.
     * @param {object} options - Opciones de configuración.
     * @param {string} options.REDIS_HOST - Host de Redis.
     * @param {number} options.REDIS_PORT - Puerto de Redis.
     * @param {string} [options.REDIS_USERNAME] - Usuario de Redis (opcional).
     * @param {string} [options.REDIS_PASSWORD] - Contraseña de Redis (opcional).
     * @param {string} [options.logLevel='info'] - Nivel mínimo de log.
     * @param {string} [options.logServiceName='QTaskApp'] - Nombre para los logs.
     * @param {boolean}[options.logUseColors=true] - Usar colores en logs.
     * @param {string} [options.logTimestampFormat='iso'] - Formato de timestamp para logs.
     * @param {object} [options.redisOptions={}] - Opciones adicionales para pasar a ioredis.
     */
    constructor(options = {}) {
        // Validar opciones básicas de Redis
        if (!options.REDIS_HOST || !options.REDIS_PORT) {
            throw new Error('QTask requiere al menos REDIS_HOST y REDIS_PORT en las opciones.');
        }

        this.config = options; // Guardar configuración original si se necesita

        // 1. Inicializar Logger
        this.log = new Logging({
            serviceName: options.logServiceName || 'QTaskApp',
            minLevel: options.logLevel || 'info',
            useColors: options.logUseColors !== false,
            timestampFormat: options.logTimestampFormat || 'iso'
        });
        this.log.info('[QTask] Logger inicializado.');

        // Las instancias de Redis, Publisher y Manager se inicializan de forma asíncrona en connect
        this.redisClient = null;
        this.publisher = null;
        this.consumerManager = null;
        this.isConnected = false;
    }

    /**
     * Establece la conexión a Redis e inicializa los componentes dependientes.
     * Debe llamarse antes de registrar o publicar.
     * @returns {Promise<void>}
     * @throws {Error} Si la conexión falla.
     */
    async connect() {
        if (this.isConnected) {
            this.log.warn('[QTask] Ya está conectado.');
            return;
        }
        this.log.info('[QTask] Estableciendo conexión Redis e inicializando componentes...');
        try {
            // Pasar solo las opciones relevantes de Redis a connectRedis
            const redisOpts = {
                 host: this.config.REDIS_HOST,
                 port: this.config.REDIS_PORT,
                 username: this.config.REDIS_USERNAME,
                 password: this.config.REDIS_PASSWORD,
                 ...(this.config.redisOptions || {}) // Mezclar opciones adicionales si se proporcionan
            };
            // Conectar usando el módulo centralizado
            this.redisClient = await connectRedis(redisOpts, this.log);
            this.isConnected = true;
            this.log.info('[QTask] Conexión Redis establecida.');

            // Inicializar Publisher y ConsumerManager DESPUÉS de conectar
            this.publisher = new Publisher({ log: this.log });
            this.consumerManager = new ConsumerManager({ log: this.log }); // Ya no necesitan redis explícito
            this.log.info('[QTask] Publisher y ConsumerManager inicializados.');

        } catch (error) {
            this.log.error('[QTask] Falló la conexión/inicialización:', error);
            this.isConnected = false;
            throw error; // Relanzar para que el flujo principal falle
        }
    }

    /**
     * Registra un handler para consumir mensajes de un topic/grupo específico.
     * @param {object} options - Opciones de registro (topic, group, handler, etc.)
     * @param {string} options.topic
     * @param {string} options.group
     * @param {function} options.handler - async (id, data, log) => {}
     * @param {string} [options.consumerId]
     * @param {number} [options.blockTimeoutMs]
     * @returns {Promise<Consumer>}
     * @throws {Error} Si no está conectado o el registro falla.
     */
    async register(options) {
        if (!this.isConnected || !this.consumerManager) {
            throw new Error('QTask no está conectado. Llama a connect() antes de registrar.');
        }
        // Delega al ConsumerManager
        return this.consumerManager.register(options);
    }

    /**
     * Publica un mensaje en un topic específico.
     * @param {string} topic
     * @param {object|string} messageData
     * @param {object} [options={}] - Opciones para XADD (ej: { id: '...' })
     * @returns {Promise<string|null>} - ID del mensaje o null si falla.
     * @throws {Error} Si no está conectado.
     */
    async publish(topic, messageData, options = {}) {
        if (!this.isConnected || !this.publisher) {
             // Podríamos lanzar un error o intentar conectar aquí si no está conectado
             throw new Error('QTask no está conectado. Llama a connect() antes de publicar.');
        }
        // Delega al Publisher
        return this.publisher.publish(topic, messageData, options);
    }

    /**
     * Detiene todos los consumidores y cierra la conexión Redis.
     * @returns {Promise<void>}
     */
    async stop() {
        this.log.info('[QTask] Iniciando proceso de detención...');
        if (this.consumerManager) {
            await this.consumerManager.stopAll();
        } else {
             this.log.info('[QTask] No hay ConsumerManager para detener.');
        }
        await disconnectRedis(); // Llama a la desconexión centralizada
        this.isConnected = false;
        this.log.info('[QTask] Proceso de detención completado.');
    }
}

export default QTask;
