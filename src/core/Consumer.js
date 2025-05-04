// src/core/Consumer.js

import { EventEmitter } from 'node:events';
// Importa la función para obtener el cliente Redis compartido
import { getClient } from './RedisConnection.js';

class Consumer extends EventEmitter {
    /**
     * Crea una instancia del consumidor de Stream, capaz de leer nuevos mensajes y reclamar pendientes.
     * @param {object} options
     * @param {Logging} options.log - Instancia del logger.
     * @param {string} options.topic - El nombre COMPLETO del stream de esta partición (ej: 'WHATSAPP:0').
     * @param {string} options.group - El nombre del grupo de consumidores.
     * @param {string} options.consumerId - Identificador único para este consumidor dentro del grupo.
     * @param {number} [options.blockTimeoutMs=2000] - Timeout (ms) para la espera de NUEVOS mensajes en XREADGROUP.
     * @param {number} [options.claimIntervalMs=300000] - Cada cuánto (ms) intentar reclamar mensajes pendientes (default: 5 minutos).
     * @param {number} [options.minIdleTimeMs=60000] - Tiempo mínimo (ms) que un mensaje debe estar pendiente para ser reclamado (default: 1 minuto).
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
        super(); // Llama al constructor de EventEmitter
        // Validar parámetros requeridos
        if (!log || !topic || !group || !consumerId) {
             throw new Error("Consumer requiere 'log', 'topic', 'group', y 'consumerId'.");
        }
        // Validar tipos y valores de parámetros opcionales
        if (!Number.isInteger(blockTimeoutMs) || blockTimeoutMs < 0) throw new Error("'blockTimeoutMs' debe ser un entero >= 0.");
        if (!Number.isInteger(claimIntervalMs) || claimIntervalMs <= 0) throw new Error("'claimIntervalMs' debe ser un entero positivo.");
        if (!Number.isInteger(minIdleTimeMs) || minIdleTimeMs < 0) throw new Error("'minIdleTimeMs' debe ser un entero >= 0.");


        this.log = log;
        this.topic = topic; // Este es el nombre completo del stream de la partición
        this.group = group;
        this.consumerId = consumerId;
        this.blockTimeoutMs = blockTimeoutMs;
        this.claimIntervalMs = claimIntervalMs;
        this.minIdleTimeMs = minIdleTimeMs;
        this.isRunning = false;
        this.claimInterval = null; // Handle para el setInterval de reclamo
    }

    /**
     * Inicia el bucle de consumo de nuevos mensajes y el chequeo periódico de mensajes pendientes.
     */
    start() {
        if (this.isRunning) {
            this.log.warn(`[Consumer:${this.consumerId}] Ya está corriendo para ${this.topic}/${this.group}.`);
            return;
        }
        this.isRunning = true;
        this.log.info(`[Consumer:${this.consumerId}] Iniciando para ${this.topic} / ${this.group}`);

        // Inicia el bucle principal para leer nuevos mensajes ('>')
        this._runLoop().catch(err => {
            this.log.error(`[Consumer:${this.consumerId}] Error fatal inesperado en _runLoop para ${this.topic}:`, err);
            this.isRunning = false;
            // Detiene el intervalo de reclamo si el loop principal falla
            if (this.claimInterval) {
                clearInterval(this.claimInterval);
                this.claimInterval = null;
            }
            this.emit('error', err, 'fatal_loop_error');
        });

        // Inicia el chequeo periódico para reclamar mensajes pendientes con XAUTOCLAIM
        // Limpia cualquier intervalo anterior que pudiera existir (por seguridad)
        if (this.claimInterval) {
            clearInterval(this.claimInterval);
        }
        // Crea el nuevo intervalo
        this.claimInterval = setInterval(async () => {
            // Solo ejecuta el reclamo si el consumidor principal sigue activo
            if (this.isRunning) {
                // Usamos un try/catch aquí para que un error en el reclamo no detenga el intervalo
                try {
                    // Llama al método que usa XAUTOCLAIM
                    await this._claimAndReprocess();
                } catch(claimError) {
                     this.log.error(`[Consumer:${this.consumerId}] Error durante ejecución periódica de _claimAndReprocess para ${this.topic}:`, claimError);
                     // Emitir error pero no detener el intervalo necesariamente
                     this.emit('error', claimError, 'claim_interval_execution');
                }
            } else {
                // Si isRunning se volvió false, limpiamos el intervalo
                if (this.claimInterval) {
                    clearInterval(this.claimInterval);
                    this.claimInterval = null;
                    this.log.info(`[Consumer:${this.consumerId}] Detenido chequeo de pendientes para ${this.topic} porque isRunning es false.`);
                }
            }
        }, this.claimIntervalMs); // Usa el intervalo configurable

        this.log.info(`[Consumer:${this.consumerId}] Programado chequeo de pendientes para ${this.topic} cada ${this.claimIntervalMs} ms (minIdleTime: ${this.minIdleTimeMs}ms).`);
    }

    /**
     * Detiene el bucle de consumo y el chequeo periódico de pendientes.
     */
    stop() {
        if (!this.isRunning) {
            // Ya está detenido o en proceso de detenerse
            return;
        }
        this.log.info(`[Consumer:${this.consumerId}] Deteniendo para ${this.topic} / ${this.group}...`);

        // Paso 1: Detener el intervalo de chequeo de pendientes
        if (this.claimInterval) {
            clearInterval(this.claimInterval);
            this.claimInterval = null; // Olvida el handle
            this.log.info(`[Consumer:${this.consumerId}] Detenido chequeo de pendientes para ${this.topic}.`);
        }

        // Paso 2: Indicar al bucle principal que debe detenerse
        // La próxima iteración del `while (this.isRunning)` en _runLoop fallará
        // y el bucle terminará limpiamente después del BLOCK timeout.
        this.isRunning = false;

        // Nota: No necesitamos forzar la cancelación de la promesa de XREADGROUP,
        // ya que el timeout de BLOCK y la verificación de isRunning se encargan.
    }

    /**
     * El bucle principal que lee NUEVOS mensajes ('>') del stream/partición asignado.
     * @private
     */
    async _runLoop() {
        while (this.isRunning) {
            let redis;
            try {
                // Intenta obtener el cliente Redis al inicio de cada ciclo de espera
                redis = getClient();

                this.log.debug(`[Consumer:${this.consumerId}] Esperando nuevos mensajes del grupo '${this.group}' en '${this.topic}'... (BLOCK ${this.blockTimeoutMs}ms)`);
                // Leer NUEVOS mensajes usando el ID especial '>'
                const result = await redis.xreadgroup(
                    'GROUP', this.group, this.consumerId,
                    'BLOCK', this.blockTimeoutMs, // Espera nuevos mensajes por este tiempo
                    'STREAMS', this.topic, '>'    // '>' significa solo mensajes nunca antes entregados a este grupo
                );

                // Si stop() fue llamado mientras estábamos bloqueados en XREADGROUP
                if (!this.isRunning) break;

                if (result) {
                    // result = [['streamName', [[messageId, [field, val,...]], ...]]]
                    const [streamName, messages] = result[0];
                    if (messages && messages.length > 0) {
                        this.log.debug(`[Consumer:${this.consumerId}] Recibidos ${messages.length} NUEVOS mensaje(s) de ${streamName}.`);
                        // Procesa los mensajes recibidos (reconstruye objeto, emite evento)
                        this._processMessages(messages);
                    }
                    // Si result tiene mensajes vacíos (raro con BLOCK > 0), simplemente sigue
                } else {
                    // Si result es null, significa que el BLOCK timeout expiró sin nuevos mensajes
                     this.log.debug(`[Consumer:${this.consumerId}] Timeout de BLOCK esperando nuevos mensajes en ${this.topic}.`);
                     // En este punto podríamos llamar a _claimAndReprocess si quisiéramos
                     // hacerlo oportunista, pero el setInterval ya lo hace periódicamente.
                }

            } catch (error) {
                // Si stop() fue llamado mientras ocurría un error
                if (!this.isRunning) break;

                // Manejo de errores específicos
                 if (error.message.includes('[RedisConnection]')) {
                     this.log.error(`[Consumer:${this.consumerId}] Error de conexión Redis en _runLoop (${this.topic}). Esperando...`);
                     this.emit('error', error, 'readloop_redis_conn');
                     await new Promise(resolve => setTimeout(resolve, Math.max(this.blockTimeoutMs, 5000))); // Espera antes de reintentar
                     continue; // Reintenta el bucle para obtener el cliente de nuevo
                 }

                 if (error.message.includes('NOGROUP No such key') || error.message.includes('no such key')) {
                     this.log.error(`[Consumer:${this.consumerId}] Stream '${this.topic}' o grupo '${this.group}' no existe. Intentando recrear grupo...`);
                     this.emit('error', new Error(`NOGROUP detected on ${this.topic}`), 'readloop_nogroup');
                     await this._tryCreateGroup(); // Intenta (re)crear el grupo en este stream
                     await new Promise(resolve => setTimeout(resolve, 5000)); // Espera antes de continuar
                 } else {
                    // Otros errores durante XREADGROUP
                    this.log.error(`[Consumer:${this.consumerId}] Error inesperado en bucle XREADGROUP para '${this.topic}':`, error);
                    this.emit('error', error, 'readloop_xreadgroup');
                    // Esperar un poco antes de reintentar para evitar spam de errores
                    await new Promise(resolve => setTimeout(resolve, 2000));
                 }
            }
        }
         // Log cuando el bucle termina limpiamente
         this.log.info(`[Consumer:${this.consumerId}] Bucle principal detenido para ${this.topic} / ${this.group}.`);
    }

    /**
     * Procesa mensajes (nuevos o reclamados), reconstruye el objeto y emite el evento 'message'.
     * @param {Array} messages - Array de mensajes. Formato: [[messageId, [field1, value1, ...]], ...]
     * @private
     */
    _processMessages(messages) {
         messages.forEach(([id, fields]) => {
            let reconstructedData = {};
            if (fields && fields.length > 0 && fields.length % 2 === 0) {
                 try {
                    for (let i = 0; i < fields.length; i += 2) {
                        reconstructedData[fields[i]] = fields[i + 1];
                    }
                     this.log.debug(`[Consumer:${this.consumerId}] Mensaje ${id} (${this.topic}) reconstruido:`, reconstructedData);
                } catch (parseError) {
                     this.log.error(`[Consumer:${this.consumerId}] Error reconstruyendo msg ${id} (${this.topic}):`, parseError);
                     this.emit('error', parseError, `parse_message_${id}`);
                     reconstructedData = null;
                }
            } else {
                this.log.warn(`[Consumer:${this.consumerId}] Formato campos inesperado ID ${id} (${this.topic}):`, fields);
                reconstructedData = null;
            }

            // Solo emitir si la reconstrucción fue exitosa
            if (reconstructedData !== null) {
                // Crear la función ack para este mensaje específico
                const ack = async () => {
                    let redisAckClient;
                    try {
                        redisAckClient = getClient();
                        await redisAckClient.xack(this.topic, this.group, id);
                        this.log.debug(`[Consumer:${this.consumerId}] Mensaje ${id} (${this.topic}) confirmado (XACK).`);
                    } catch (ackError) {
                         if (ackError.message.includes('[RedisConnection]')) {
                             this.log.error(`[Consumer:${this.consumerId}] Falló ACK para ${id} (${this.topic}) - Redis desconectado.`);
                         } else {
                            this.log.error(`[Consumer:${this.consumerId}] Error ACK mensaje ${id} (${this.topic}):`, ackError);
                         }
                        this.emit('error', ackError, `ack_${id}`);
                    }
                };
                // Emitir el evento con id, datos reconstruidos y la función ack
                this.emit('message', id, reconstructedData, ack);
            }
        });
    }


    /**
     * Intenta reclamar y procesar mensajes pendientes que han estado inactivos usando XAUTOCLAIM.
     * @private
     */
    async _claimAndReprocess() {
        this.log.debug(`[Consumer:${this.consumerId}] Ejecutando chequeo de pendientes para ${this.topic} (minIdleTime: ${this.minIdleTimeMs}ms)...`);
        const startId = '0-0'; // Escanear desde el inicio de la lista de pendientes (PEL)
        const count = 10;     // Reclamar hasta 10 mensajes a la vez

        let redis;
        try {
            // Obtener cliente Redis
            redis = getClient();

            // Ejecutar XAUTOCLAIM
            const claimResult = await redis.xautoclaim(
                this.topic,          // El stream/partición de este consumidor
                this.group,          // El grupo
                this.consumerId,     // Este consumidor reclama para sí mismo
                this.minIdleTimeMs,  // Tiempo mínimo de inactividad
                startId,             // ID inicial para escanear la PEL
                'COUNT', count       // Límite de mensajes a reclamar
            );

            // El resultado es un array: [nextStartId, claimedMessagesArray, ?deletedEntryIds?]
            const nextStartId = claimResult[0];
            const claimedMessages = claimResult[1]; // Formato: [[messageId, [field, val,...]], ...]

            if (claimedMessages && claimedMessages.length > 0) {
                this.log.info(`[Consumer:${this.consumerId}] Reclamados ${claimedMessages.length} mensajes pendientes de ${this.topic}.`);
                // Procesar los mensajes reclamados usando la misma lógica que los nuevos
                this._processMessages(claimedMessages);
            } else {
                 this.log.debug(`[Consumer:${this.consumerId}] No se encontraron mensajes pendientes (> ${this.minIdleTimeMs}ms) para reclamar en ${this.topic}.`);
            }

            // Nota: Podríamos usar nextStartId para continuar reclamando si claimedMessages.length === count,
            // pero para un chequeo periódico, empezar desde '0-0' suele ser suficiente.

        } catch (error) {
             if (error.message.includes('[RedisConnection]')) {
                 this.log.error(`[Consumer:${this.consumerId}] No se pudo ejecutar XAUTOCLAIM (${this.topic}) - Redis desconectado.`);
                 this.emit('error', error, 'autoclaim_redis_conn');
             } else if (error.message.includes('NOGROUP')) {
                 // Es posible que el grupo no exista si el stream fue borrado/recreado
                 this.log.warn(`[Consumer:${this.consumerId}] Grupo ${this.group} no encontrado durante XAUTOCLAIM en ${this.topic}. Intentando recrear.`);
                 this.emit('error', error, 'autoclaim_nogroup');
                 await this._tryCreateGroup(); // Intenta recrear el grupo
             } else if (error.message.includes('ERR XAUTOCLAIM requires Redis >= 6.2.0')) {
                  this.log.error(`[Consumer:${this.consumerId}] XAUTOCLAIM no soportado por esta versión de Redis. Deshabilitando chequeo de pendientes para ${this.topic}.`);
                  // Detener el intervalo para no seguir fallando
                  this.stop(); // Llama a stop para limpiar el intervalo
                  this.emit('error', error, 'autoclaim_unsupported');
             }
             else {
                // Otros errores (ej. permisos, sintaxis si cambia versión Redis)
                this.log.error(`[Consumer:${this.consumerId}] Error inesperado durante XAUTOCLAIM en ${this.topic}:`, error);
                this.emit('error', error, 'autoclaim');
            }
        }
    }


    /**
     * Intenta crear el grupo de consumidores en el stream de esta instancia.
     * @private
     */
     async _tryCreateGroup() {
        let redisGroupClient;
        try {
            redisGroupClient = getClient();
            // Usar this.topic que ya es el nombre completo del stream de la partición
            await redisGroupClient.xgroup('CREATE', this.topic, this.group, '0', 'MKSTREAM');
            this.log.info(`[Consumer:${this.consumerId}] Grupo '${this.group}' creado/asegurado internamente en '${this.topic}'.`);
        } catch (error) {
             if (error.message.includes('BUSYGROUP')) {
                this.log.info(`[Consumer:${this.consumerId}] Grupo '${this.group}' ya existía en '${this.topic}' (verificado).`);
            } else if (error.message.includes('[RedisConnection]')) {
                 this.log.error(`[Consumer:${this.consumerId}] No se pudo crear grupo en '${this.topic}' - Redis desconectado.`);
                 this.emit('error', error, 'creategroup_noconn');
            } else {
                 this.log.error(`[Consumer:${this.consumerId}] Error interno al intentar crear grupo '${this.group}' en '${this.topic}':`, error);
                 this.emit('error', error, 'creategroup');
                 // No relanzar aquí para permitir que el bucle principal intente de nuevo
            }
        }
     }
}

export default Consumer;