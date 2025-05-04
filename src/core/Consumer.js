import { EventEmitter } from 'node:events';
// Importa la función para obtener el cliente Redis compartido
import { getClient } from './RedisConnection.js';

class Consumer extends EventEmitter {
    /**
     * Crea una instancia del consumidor de Stream.
     * Ya NO recibe 'redis' directamente.
     * @param {object} options
     * @param {Logging} options.log - Instancia del logger.
     * @param {string} options.topic - El stream (topic) a consumir.
     * @param {string} options.group - El grupo de consumidores.
     * @param {string} options.consumerId - Identificador único para este consumidor dentro del grupo.
     * @param {number} [options.blockTimeoutMs=2000] - Tiempo (ms) que XREADGROUP esperará por mensajes.
     */
    constructor({ log, topic, group, consumerId, blockTimeoutMs = 2000 }) {
        super(); // Llama al constructor de EventEmitter
        // Valida que los parámetros necesarios estén presentes
        if (!log || !topic || !group || !consumerId) {
             throw new Error("Consumer requiere 'log', 'topic', 'group', y 'consumerId'.");
        }
        this.log = log;
        this.topic = topic;
        this.group = group;
        this.consumerId = consumerId;
        this.blockTimeoutMs = blockTimeoutMs;
        this.isRunning = false;
        // No almacena this.redis
    }

    /**
     * Inicia el bucle de consumo de mensajes.
     */
    start() {
        if (this.isRunning) {
            this.log.warn(`[Consumer:${this.consumerId}] Ya está corriendo para ${this.topic}/${this.group}.`);
            return;
        }
        this.isRunning = true;
        this.log.info(`[Consumer:${this.consumerId}] Iniciando para ${this.topic} / ${this.group}`);
        // Inicia el bucle en segundo plano (no usar await aquí deliberadamente)
        this._runLoop().catch(err => {
            // Captura errores no manejados directamente del bucle si ocurren
            this.log.error(`[Consumer:${this.consumerId}] Error fatal inesperado en _runLoop:`, err);
            this.isRunning = false; // Asegura que se marque como detenido
            this.emit('error', err, 'fatal_loop_error');
        });
    }

    /**
     * Detiene el bucle de consumo de mensajes.
     */
    stop() {
        if (!this.isRunning) {
            // Ya está detenido o deteniéndose
            return;
        }
        this.log.info(`[Consumer:${this.consumerId}] Deteniendo para ${this.topic} / ${this.group}...`);
        this.isRunning = false;
        // La comprobación `while (this.isRunning)` y el timeout de BLOCK
        // se encargarán de detener el bucle en la siguiente iteración.
    }

    /**
     * El bucle principal que lee continuamente del stream.
     * @private
     */
    async _runLoop() {
        while (this.isRunning) {
            let redis; // Declara la variable para el cliente Redis en este ciclo
            try {
                // Intenta obtener el cliente Redis al inicio de cada ciclo
                redis = getClient();

                this.log.debug(`[Consumer:${this.consumerId}] Esperando mensajes del grupo '${this.group}'... (BLOCK ${this.blockTimeoutMs}ms)`);
                const result = await redis.xreadgroup( // Usa el cliente obtenido
                    'GROUP', this.group, this.consumerId,
                    'BLOCK', this.blockTimeoutMs,
                    'STREAMS', this.topic, '>'
                );

                // Si el consumidor fue detenido mientras esperaba (BLOCK)
                if (!this.isRunning) break;

                if (result) {
                    // Resultado contiene [[streamName, [[messageId, [field1, value1, ...]], ...]]]
                    const [streamName, messages] = result[0];
                    if (messages && messages.length > 0) {
                        // Llama a _processMessages (que también obtendrá el cliente para XACK)
                        this._processMessages(messages);
                    }
                    // Si result es null o messages está vacío, simplemente continuamos el bucle (timeout)
                } else {
                    // Timeout de BLOCK, continuamos el bucle para volver a intentar
                     this.log.debug(`[Consumer:${this.consumerId}] Timeout de BLOCK, reintentando lectura.`);
                }

            } catch (error) {
                // Si fue detenido mientras ocurría un error
                if (!this.isRunning) break;

                // Manejar error si getClient() falla (Redis no conectado)
                 if (error.message.includes('[RedisConnection]')) {
                     this.log.error(`[Consumer:${this.consumerId}] No se pudo leer - Redis no conectado. Esperando...`);
                     // Esperar un tiempo antes de reintentar el bucle
                     await new Promise(resolve => setTimeout(resolve, Math.max(this.blockTimeoutMs, 5000))); // Espera al menos 5s o el timeout
                     continue; // Saltar al siguiente ciclo para intentar obtener el cliente de nuevo
                 }

                // Manejar error NOGROUP (Stream o Grupo no existe)
                if (error.message.includes('NOGROUP No such key') || error.message.includes('no such key')) {
                     this.log.error(`[Consumer:${this.consumerId}] Stream '${this.topic}' o grupo '${this.group}' no existe. Reintentando creación de grupo...`);
                     this.emit('error', new Error('NOGROUP detected'), 'readloop_nogroup');
                     // Intenta crear el grupo (usará getClient internamente)
                     await this._tryCreateGroup();
                     // Espera un tiempo prudencial después de intentar crear el grupo
                     await new Promise(resolve => setTimeout(resolve, 5000));
                } else {
                    // Otros errores durante XREADGROUP
                    this.log.error(`[Consumer:${this.consumerId}] Error en bucle XREADGROUP para '${this.group}':`, error);
                    this.emit('error', error, 'readloop_xreadgroup');
                    // Esperar un poco en caso de otros errores antes de reintentar
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }
        }
         this.log.info(`[Consumer:${this.consumerId}] Bucle de consumo detenido para ${this.topic} / ${this.group}.`);
    }

    /**
     * Procesa los mensajes recibidos, reconstruye el objeto JS y emite el evento 'message'.
     * @param {Array} messages - Array de mensajes de XREADGROUP. Formato: [[messageId, [field1, value1, ...]], ...]
     * @private
     */
    _processMessages(messages) {
         messages.forEach(([id, fields]) => { // Desestructura el id y los fields del mensaje
            let reconstructedData = {}; // Objeto para guardar los datos reconstruidos

            // Verifica si 'fields' es un array plano válido (par key-value)
            if (fields && fields.length > 0 && fields.length % 2 === 0) {
                try {
                    for (let i = 0; i < fields.length; i += 2) {
                        reconstructedData[fields[i]] = fields[i + 1];
                    }
                     this.log.debug(`[Consumer:${this.consumerId}] Mensaje ${id} reconstruido a objeto:`, reconstructedData);
                } catch (parseError) {
                     this.log.error(`[Consumer:${this.consumerId}] Error al reconstruir objeto del mensaje ${id}:`, parseError);
                     this.emit('error', parseError, `parse_message_${id}`);
                     reconstructedData = null; // Indica fallo en la reconstrucción
                }
            } else {
                this.log.warn(`[Consumer:${this.consumerId}] Formato de campos inesperado para ID ${id}, no se pudo reconstruir objeto. Campos:`, fields);
                reconstructedData = null; // Indica fallo en la reconstrucción
            }

            // Solo emite 'message' si la reconstrucción fue exitosa
            if (reconstructedData !== null) {
                // Define la función ack que se pasará al listener del evento 'message'
                const ack = async () => {
                    let redisAckClient;
                    try {
                        // Obtiene el cliente Redis para ejecutar XACK
                        redisAckClient = getClient();
                        await redisAckClient.xack(this.topic, this.group, id);
                        this.log.debug(`[Consumer:${this.consumerId}] Mensaje ${id} confirmado (XACK).`);
                    } catch (ackError) {
                         // Manejar error si getClient falla durante ACK
                         if (ackError.message.includes('[RedisConnection]')) {
                             this.log.error(`[Consumer:${this.consumerId}] No se pudo hacer ACK para ${id} - Redis desconectado.`);
                             // Podrías reintentar el ACK más tarde o marcarlo para revisión
                         } else {
                            this.log.error(`[Consumer:${this.consumerId}] Error al confirmar (XACK) mensaje ${id}:`, ackError);
                         }
                         // Emitir error de ACK para que la aplicación principal lo sepa
                        this.emit('error', ackError, `ack_${id}`);
                    }
                };

                // Emitir el evento 'message' con el id, el objeto reconstruido y la función ack
                this.emit('message', id, reconstructedData, ack);
            }
            // Si reconstructedData es null, el mensaje malformado simplemente se ignora aquí.
            // Podría añadirse un evento 'malformed_message' si se quisiera tratar explícitamente.
        });
    }

    /**
     * Intenta crear el grupo de consumidores si no existe.
     * @private
     */
     async _tryCreateGroup() {
        let redisGroupClient;
        try {
            // Obtiene el cliente Redis para ejecutar XGROUP
            redisGroupClient = getClient();
            await redisGroupClient.xgroup('CREATE', this.topic, this.group, '0', 'MKSTREAM');
            this.log.info(`[Consumer:${this.consumerId}] Grupo '${this.group}' creado internamente en stream '${this.topic}'.`);
        } catch (error) {
            if (error.message.includes('BUSYGROUP')) {
                this.log.info(`[Consumer:${this.consumerId}] Grupo '${this.group}' ya existía (verificado internamente).`);
            } else if (error.message.includes('[RedisConnection]')) {
                 // Error específico si no se pudo obtener el cliente Redis
                 this.log.error(`[Consumer:${this.consumerId}] No se pudo crear grupo '${this.group}' - Redis desconectado.`);
                 this.emit('error', error, 'creategroup_noconn');
            }
             else {
                 // Otros errores durante XGROUP CREATE
                 this.log.error(`[Consumer:${this.consumerId}] Error interno al intentar crear grupo '${this.group}':`, error);
                 this.emit('error', error, 'creategroup');
            }
            // No relanzamos el error aquí para permitir que el bucle principal reintente
        }
    }
}

export default Consumer;