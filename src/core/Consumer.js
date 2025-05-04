import { EventEmitter } from 'node:events';
import { getClient } from './RedisConnection.js';

class Consumer extends EventEmitter {
    /**
     * @param {object} options
     * @param {Logging} options.log - Instancia del logger.
     * @param {string} options.topic - Stream a consumir.
     * @param {string} options.group - Grupo de consumidores.
     * @param {string} options.consumerId - ID de este consumidor.
     * @param {number} [options.blockTimeoutMs=2000] - Timeout para XREADGROUP.
     * @param {number} [options.claimIntervalMs=300000] - Cada cuánto (ms) intentar reclamar mensajes pendientes (e.g., 300000 = 5 minutos).
     * @param {number} [options.minIdleTimeMs=60000] - Tiempo mínimo (ms) que un mensaje debe estar pendiente para ser reclamado (e.g., 60000 = 1 minuto).
     */
    constructor({
        log,
        topic,
        group,
        consumerId,
        blockTimeoutMs = 2000,
        claimIntervalMs = 300000, // Default 5 minutos
        minIdleTimeMs = 60000   // Default 1 minuto
    }) {
        super();
        if (!log || !topic || !group || !consumerId) {
            throw new Error("Consumer requiere 'log', 'topic', 'group', y 'consumerId'.");
        }
        this.log = log;
        this.topic = topic;
        this.group = group;
        this.consumerId = consumerId;
        this.blockTimeoutMs = blockTimeoutMs;
        this.claimIntervalMs = claimIntervalMs; // Guardar intervalo de reclamo
        this.minIdleTimeMs = minIdleTimeMs;     // Guardar tiempo mínimo de inactividad
        this.isRunning = false;
        this.claimInterval = null; // Handle para el setInterval de reclamo
    }

    /**
     * Inicia el bucle de consumo y el chequeo periódico de pendientes.
     */
    start() {
        if (this.isRunning) {
            this.log.warn(`[Consumer:${this.consumerId}] Ya está corriendo para ${this.topic}/${this.group}.`);
            return;
        }
        this.isRunning = true;
        this.log.info(`[Consumer:${this.consumerId}] Iniciando para ${this.topic} / ${this.group}`);

        // Inicia el bucle principal en segundo plano
        this._runLoop().catch(err => {
            this.log.error(`[Consumer:${this.consumerId}] Error fatal inesperado en _runLoop:`, err);
            this.isRunning = false;
            if (this.claimInterval) clearInterval(this.claimInterval); // Detiene reclamo si el loop principal falla
            this.emit('error', err, 'fatal_loop_error');
        });

        // Inicia el chequeo periódico para reclamar mensajes pendientes
        // Limpia intervalo anterior si existiera (seguridad)
        if (this.claimInterval) {
            clearInterval(this.claimInterval);
        }
        // Inicia el nuevo intervalo
        this.claimInterval = setInterval(async () => {
            if (this.isRunning) {
                // Llama al método de reclamo de forma asíncrona
                await this._claimAndReprocess().catch(err => {
                    // Captura errores directamente de la llamada periódica
                    this.log.error(`[Consumer:${this.consumerId}] Error durante ejecución periódica de _claimAndReprocess:`, err);
                });
            } else {
                // Si el consumidor ya no está corriendo, detenemos el intervalo
                if (this.claimInterval) {
                    clearInterval(this.claimInterval);
                    this.claimInterval = null;
                    this.log.info(`[Consumer:${this.consumerId}] Detenido chequeo de pendientes porque isRunning es false.`);
                }
            }
        }, this.claimIntervalMs); // Usa el intervalo configurable

        this.log.info(`[Consumer:${this.consumerId}] Programado chequeo de pendientes cada ${this.claimIntervalMs} ms (minIdleTime: ${this.minIdleTimeMs}ms).`);
    }

    /**
     * Detiene el bucle de consumo y el chequeo de pendientes.
     */
    stop() {
        if (!this.isRunning) {
            return;
        }
        this.log.info(`[Consumer:${this.consumerId}] Deteniendo para ${this.topic} / ${this.group}...`);

        // Detener el intervalo de chequeo de pendientes
        if (this.claimInterval) {
            clearInterval(this.claimInterval);
            this.claimInterval = null;
            this.log.info(`[Consumer:${this.consumerId}] Detenido chequeo de pendientes.`);
        }

        // Marcar para detener el bucle principal
        this.isRunning = false;
    }

    /**
     * El bucle principal que lee nuevos mensajes.
     * @private
     */
    async _runLoop() {
        while (this.isRunning) {
            let redis;
            try {
                redis = getClient();
                this.log.debug(`[Consumer:${this.consumerId}] Esperando nuevos mensajes del grupo '${this.group}'... (BLOCK ${this.blockTimeoutMs}ms)`);
                // Leer NUEVOS mensajes ('>')
                const result = await redis.xreadgroup(
                    'GROUP', this.group, this.consumerId,
                    'BLOCK', this.blockTimeoutMs,
                    'STREAMS', this.topic, '>' // Lee solo nuevos mensajes
                );

                if (!this.isRunning) break;

                if (result) {
                    const [streamName, messages] = result[0];
                    if (messages && messages.length > 0) {
                        this.log.debug(`[Consumer:${this.consumerId}] Recibidos ${messages.length} NUEVOS mensaje(s).`);
                        this._processMessages(messages); // Procesa los mensajes nuevos
                    }
                } else {
                    this.log.debug(`[Consumer:${this.consumerId}] Timeout de BLOCK para nuevos mensajes.`);
                    // Podríamos llamar a _claimAndReprocess aquí también si quisiéramos,
                    // pero ya tenemos el setInterval independiente.
                }

            } catch (error) {
                // ... (manejo de errores de _runLoop sin cambios) ...
                if (!this.isRunning) break;
                if (error.message.includes('[RedisConnection]')) { /* ... */ await new Promise(resolve => setTimeout(resolve, Math.max(this.blockTimeoutMs, 5000))); continue; }
                if (error.message.includes('NOGROUP')) { /* ... */ await this._tryCreateGroup(); await new Promise(resolve => setTimeout(resolve, 5000)); }
                else { /* ... */ await new Promise(resolve => setTimeout(resolve, 2000)); }
            }
        }
        this.log.info(`[Consumer:${this.consumerId}] Bucle principal detenido para ${this.topic} / ${this.group}.`);
    }

    /**
     * Procesa mensajes (ya sean nuevos o reclamados) y emite eventos.
     * @param {Array} messages - Array de mensajes de XREADGROUP o XAUTOCLAIM.
     * @private
     */
    _processMessages(messages) {
        // ... (Lógica interna de _processMessages para reconstruir objeto y emitir evento con 'ack' SIN CAMBIOS) ...
        messages.forEach(([id, fields]) => {
            let reconstructedData = {};
            if (fields && fields.length > 0 && fields.length % 2 === 0) {
                try {
                    for (let i = 0; i < fields.length; i += 2) {
                        reconstructedData[fields[i]] = fields[i + 1];
                    }
                    this.log.debug(`[Consumer:${this.consumerId}] Mensaje ${id} reconstruido:`, reconstructedData);
                } catch (parseError) { /* ... */ reconstructedData = null; }
            } else {
                this.log.warn(`[Consumer:${this.consumerId}] Formato inesperado ID ${id}`);
                reconstructedData = null;
            }

            if (reconstructedData !== null) {
                const ack = async () => { /* ... lógica de ack sin cambios ... */ };
                this.emit('message', id, reconstructedData, ack);
            }
        });
    }


    /**
     * Intenta reclamar y procesar mensajes pendientes que han estado inactivos.
     * @private
     */
    async _claimAndReprocess() {
        this.log.debug(`[Consumer:${this.consumerId}] Ejecutando chequeo de mensajes pendientes (minIdleTime: ${this.minIdleTimeMs}ms)...`);
        const startId = '0-0'; // Empezar a escanear desde el inicio de la PEL
        const count = 10;     // Reclamar hasta 10 mensajes por chequeo

        let redis;
        try {
            redis = getClient();
            // Llama a XAUTOCLAIM
            const claimResult = await redis.xautoclaim(
                this.topic,
                this.group,
                this.consumerId,    // Este consumidor reclama para sí mismo
                this.minIdleTimeMs, // Tiempo mínimo de inactividad para reclamar
                startId,            // Escanear desde el principio de pendientes
                'COUNT', count      // Limitar cantidad por llamada
            );

            // claimResult = [nextStartId, claimedMessagesArray, /* deletedIds (en Redis 7+) */]
            const nextStartId = claimResult[0]; // Para escaneos continuos si se quisiera
            const claimedMessages = claimResult[1]; // Array de [[messageId, [field, val,...]], ...]

            if (claimedMessages && claimedMessages.length > 0) {
                this.log.info(`[Consumer:${this.consumerId}] Reclamados ${claimedMessages.length} mensajes pendientes.`);
                // El formato de claimedMessages es idéntico al de XREADGROUP,
                // podemos usar la misma función para procesarlos y emitir el evento.
                this._processMessages(claimedMessages);
            } else {
                // Es normal que no haya nada que reclamar la mayoría de las veces
                this.log.debug(`[Consumer:${this.consumerId}] No se encontraron mensajes pendientes para reclamar.`);
            }

            // Podrías usar nextStartId si necesitaras reclamar más de 'count' mensajes
            // en un solo ciclo, llamando a xautoclaim de nuevo con ese ID.
            // if (nextStartId !== '0-0' && claimedMessages.length === count) { /* lógica para reclamar más */}

        } catch (error) {
            // Manejar error si getClient falla o si XAUTOCLAIM falla
            if (error.message.includes('[RedisConnection]')) {
                this.log.error(`[Consumer:${this.consumerId}] No se pudo ejecutar XAUTOCLAIM - Redis desconectado.`);
            } else if (error.message.includes('NOGROUP')) {
                // Si el grupo no existe, no hay nada que reclamar. Intentar recrear.
                this.log.warn(`[Consumer:${this.consumerId}] Grupo ${this.group} no encontrado durante XAUTOCLAIM.`);
                await this._tryCreateGroup();
            }
            else {
                // Otros errores de XAUTOCLAIM
                this.log.error(`[Consumer:${this.consumerId}] Error durante XAUTOCLAIM:`, error);
            }
            this.emit('error', error, 'autoclaim');
        }
    }


    /**
     * Intenta crear el grupo de consumidores.
     * @private
     */
    async _tryCreateGroup() {
        // ... (lógica de _tryCreateGroup sin cambios, usa getClient internamente) ...
        let redisGroupClient;
        try {
            redisGroupClient = getClient();
            await redisGroupClient.xgroup('CREATE', this.topic, this.group, '0', 'MKSTREAM');
            this.log.info(`[Consumer:${this.consumerId}] Grupo '${this.group}' creado internamente.`);
        } catch (error) { /* ... manejo de errores sin cambios ... */ }
    }
}

export default Consumer;