import Consumer from './Consumer.js';
// Importa la función para obtener el cliente Redis compartido
import { getClient } from './RedisConnection.js';

class ConsumerManager {
    /**
     * Constructor actualizado: Solo requiere 'log'.
     * @param {object} options
     * @param {Logging} options.log - Instancia del logger principal.
     */
    constructor({ log }) {
        // Ya no recibe ni espera 'redis'
        if (!log) {
            throw new Error("ConsumerManager requiere una instancia de 'log'.");
        }
        this.log = log;
        this.consumers = new Map(); // Almacena los consumidores activos
    }

    /**
     * Intenta crear el grupo de consumidores si no existe.
     * Usa getClient() para obtener la instancia de Redis.
     * @private
     * @param {string} topic
     * @param {string} group
     * @throws {Error} Si no se puede obtener el cliente Redis o si falla la creación del grupo por otra razón que no sea BUSYGROUP.
     */
    async #createGroupIfNotExists(topic, group) {
        let redis;
        try {
            redis = getClient(); // Obtiene el cliente al inicio del método
            await redis.xgroup('CREATE', topic, group, '0', 'MKSTREAM');
            this.log.info(`[Manager] Grupo de consumidores '${group}' creado en stream '${topic}'.`);
        } catch (error) {
            if (error.message.includes('BUSYGROUP')) {
                this.log.info(`[Manager] El grupo de consumidores '${group}' ya existe en stream '${topic}'.`);
            } else if (error.message.includes('[RedisConnection]')) {
                 // Error específico si getClient falló
                 this.log.error(`[Manager] No se pudo crear grupo ${group} - Redis desconectado.`);
                 throw error; // Relanzar para que el registro falle
            }
             else {
                 // Otro error durante XGROUP CREATE
                this.log.error(`[Manager] Error al intentar crear/verificar grupo '${group}':`, error);
                throw error; // Relanzar para que el registro falle
            }
        }
    }

    /**
     * Registra y inicia un consumidor para un topic/grupo con un handler específico.
     * @param {object} options
     * @param {string} options.topic - El stream a escuchar.
     * @param {string} options.group - El grupo de consumidores.
     * @param {function} options.handler - La función async (id, data, log) => {} que procesará el mensaje.
     * @param {string} [options.consumerId] - ID opcional para el consumidor. Se genera uno si no se provee.
     * @param {number} [options.blockTimeoutMs=5000] - Timeout para XREADGROUP.
     * @returns {Promise<Consumer>} - Promesa que resuelve con la instancia del consumidor iniciada.
     * @throws {Error} Si falla la creación del grupo o falta algún parámetro.
     */
    async register({ topic, group, handler, consumerId, blockTimeoutMs = 5000 }) {
        if (!topic || !group || typeof handler !== 'function') {
            throw new Error("Registro requiere 'topic', 'group' y una función 'handler'.");
        }

        const id = consumerId || `consumer-${group}-${process.pid}-${Date.now()}`;
        const consumerKey = `${topic}:${group}:${id}`;

        if (this.consumers.has(consumerKey)) {
            this.log.warn(`[Manager] Ya existe un consumidor registrado para ${consumerKey}.`);
            return this.consumers.get(consumerKey);
        }

        this.log.info(`[Manager] Registrando consumidor para ${consumerKey}...`);

        // 1. Asegurar que el grupo exista (llama al método privado actualizado)
        // Si esto falla (ej. Redis desconectado), la excepción detendrá el registro.
        await this.#createGroupIfNotExists(topic, group);

        // 2. Crear instancia de Consumer (ya no le pasamos redis)
        const consumerInstance = new Consumer({
            log: this.log, // Pasa el logger
            topic,
            group,
            consumerId: id,
            blockTimeoutMs
        });

        // 3. Adjuntar el handler proporcionado al evento 'message'
        consumerInstance.on('message', async (messageId, messageData, ack) => {
            const handlerLog = this.log; // O un logger hijo si prefieres
            handlerLog.debug(`[Manager] Pasando mensaje ${messageId} al handler para ${consumerKey}`);
            try {
                // Llama al handler proporcionado por el usuario
                await handler(messageId, messageData, handlerLog);
                // Si el handler termina sin error, llamamos a ack
                await ack();
            } catch (processingError) {
                // Si el handler lanza un error, lo logueamos y NO llamamos a ack
                handlerLog.error(`[Handler:${id}] Error procesando mensaje ${messageId}:`, processingError);
                // El mensaje quedará pendiente en Redis
            }
        });

        // 4. Adjuntar handler genérico de errores del consumidor
        consumerInstance.on('error', (error, context) => {
            // Loguear errores internos del Consumer (ej: fallo de ACK, NOGROUP recurrente)
            this.log.error(`[ConsumerError:${id}] Contexto [${context || 'general'}]:`, error.message);
            // Aquí podrías añadir lógica de notificación o reintento si es necesario
        });

        // 5. Iniciar el consumidor (el consumidor internamente usará getClient)
        consumerInstance.start();

        // 6. Almacenar y retornar la instancia
        this.consumers.set(consumerKey, consumerInstance);
        this.log.info(`[Manager] Consumidor ${consumerKey} iniciado y escuchando.`);
        return consumerInstance;
    }

    /**
     * Detiene todos los consumidores gestionados.
     * @returns {Promise<void>}
     */
    async stopAll() {
        this.log.info('[Manager] Deteniendo todos los consumidores...');
        const stopPromises = [];
        let longestTimeout = 0;
        for (const [key, consumer] of this.consumers.entries()) {
            this.log.info(`[Manager] Solicitando detención para ${key}...`);
            consumer.stop(); // Llama al stop de la instancia Consumer
            longestTimeout = Math.max(longestTimeout, consumer.blockTimeoutMs || 0);
            stopPromises.push(key);
        }
        this.consumers.clear();
        this.log.info(`[Manager] Solicitud de detención enviada para ${stopPromises.length} consumidores.`);
        // Esperar un poco más que el timeout más largo para asegurar la detención
        await new Promise(resolve => setTimeout(resolve, longestTimeout + 500));
        this.log.info('[Manager] Tiempo de espera para detención completado.');
    }

     /**
     * Detiene un consumidor específico por su clave.
     * @param {string} key - La clave "topic:group:id" del consumidor a detener.
     * @returns {Promise<void>}
     */
    async stop(key) {
        if (this.consumers.has(key)) {
            this.log.info(`[Manager] Deteniendo consumidor específico ${key}...`);
            const consumer = this.consumers.get(key);
            const timeout = consumer.blockTimeoutMs || 2000; // Usa el timeout del consumidor
            consumer.stop();
            this.consumers.delete(key);
            await new Promise(resolve => setTimeout(resolve, timeout + 500)); // Espera basada en su timeout
            this.log.info(`[Manager] Consumidor ${key} detenido.`);
        } else {
            this.log.warn(`[Manager] No se encontró consumidor con clave ${key} para detener.`);
        }
    }
}

export default ConsumerManager;