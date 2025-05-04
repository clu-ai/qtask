import Consumer from './Consumer.js';
// Importa la función para obtener el cliente Redis compartido
import { getClient } from './RedisConnection.js';
// No necesita importar Partitioner si se lo pasan en el constructor

class ConsumerManager {
    /**
     * Constructor para el gestor de consumidores con particionamiento.
     * @param {object} options
     * @param {Logging} options.log - Instancia del logger principal.
     * @param {Partitioner} options.partitioner - Instancia del Partitioner configurada con el total de particiones.
     * @param {number} options.totalPartitions - Número total de particiones del sistema (debe coincidir con el partitioner).
     */
    constructor({ log, partitioner, totalPartitions }) {
        if (!log || !partitioner || !totalPartitions) {
            throw new Error("ConsumerManager requiere instancias de 'log', 'partitioner' y el número 'totalPartitions'.");
        }
        if (partitioner.totalPartitions !== totalPartitions) {
             throw new Error(`Inconsistencia: ConsumerManager totalPartitions (${totalPartitions}) no coincide con Partitioner totalPartitions (${partitioner.totalPartitions}).`);
        }
        this.log = log;
        this.partitioner = partitioner;
        this.totalPartitions = totalPartitions;
        // Almacena consumidores activos. Clave: streamName (topic:index):group:ID
        this.consumers = new Map();
    }

    /**
     * Determina qué índices de partición debe manejar esta instancia/pod.
     * @param {number} instanceId - ID de esta instancia (0-based).
     * @param {number} instanceCount - Total de instancias/pods corriendo para este grupo.
     * @returns {number[]} - Array de índices de partición asignados.
     * @private
     */
    #getAssignedPartitionIndices(instanceId, instanceCount) {
        const assignedIndices = [];
        if (!Number.isInteger(instanceCount) || instanceCount <= 0) {
            this.log.error(`[Manager] instanceCount inválido (${instanceCount}), no se pueden asignar particiones.`);
            return [];
        }
        if (!Number.isInteger(instanceId) || instanceId < 0) {
             this.log.error(`[Manager] instanceId inválido (${instanceId}).`);
            return [];
        }

        this.log.debug(`[Manager] Calculando particiones para instancia ${instanceId} de ${instanceCount} (Total Particiones: ${this.totalPartitions})`);
        for (let i = 0; i < this.totalPartitions; i++) {
            if (i % instanceCount === instanceId) {
                assignedIndices.push(i);
            }
        }
        return assignedIndices;
    }

    /**
     * Intenta crear el grupo de consumidores en un stream de partición específico.
     * @param {string} topic - El nombre BASE del topic (ej: 'WHATSAPP'). <--- Parámetro renombrado
     * @param {string} group - El nombre del grupo.
     * @param {number} partitionIndex - El índice de la partición.
     * @private
     * @throws {Error} Si la creación falla.
     */
    async #ensureGroupOnPartition(topic, group, partitionIndex) { // <-- Parámetro renombrado
        // Construye el nombre completo del stream usando el topic base
        const streamName = this.partitioner.getPartitionStreamName(topic, partitionIndex); // <-- Usa 'topic'
        let redis;
        try {
            redis = getClient();
            this.log.debug(`[Manager] Asegurando grupo '${group}' en partición '${streamName}'...`);
            await redis.xgroup('CREATE', streamName, group, '0', 'MKSTREAM');
            this.log.info(`[Manager] Grupo '${group}' asegurado/creado en partición '${streamName}'.`);
        } catch (error) {
            if (error.message.includes('BUSYGROUP Consumer Group name already exists')) {
                this.log.info(`[Manager] Grupo '${group}' ya existe en partición '${streamName}'.`);
            } else if (error.message.includes('[RedisConnection]')) {
                 this.log.error(`[Manager] No se pudo asegurar grupo en ${streamName} - Redis desconectado.`);
                 throw error;
            } else {
                this.log.error(`[Manager] Error inesperado al asegurar grupo '${group}' en partición '${streamName}':`, error);
                throw error;
            }
        }
    }

    /**
     * Registra handlers y crea/inicia Consumers para las particiones asignadas a esta instancia.
     * @param {object} options
     * @param {string} options.topic - Nombre base del topic (sin índice de partición). <--- Ajustado en JSDoc
     * @param {string} options.group - Nombre del grupo de consumidores (común a todas las particiones).
     * @param {function} options.handler - Función async (id, data, log, partitionIndex) => {} que procesará los mensajes.
     * @param {object} [options.partitioning] - Configuración para determinar qué particiones manejar.
     * @param {number} [options.partitioning.instanceId=process.env.INSTANCE_ID || 0] - ID (0-based) de esta instancia/pod.
     * @param {number} [options.partitioning.instanceCount=process.env.INSTANCE_COUNT || 1] - Total de instancias/pods corriendo.
     * @param {string} [options.consumerIdBase] - Prefijo para el ID del consumidor (se le añadirá el índice de partición).
     * @param {number} [options.blockTimeoutMs=5000] - Timeout para XREADGROUP pasado al Consumer.
     * @param {number} [options.claimIntervalMs=300000] - Intervalo de reclamo pasado al Consumer.
     * @param {number} [options.minIdleTimeMs=60000] - Tiempo de inactividad para reclamo pasado al Consumer.
     * @returns {Promise<void>}
     * @throws {Error} Si faltan parámetros, la config de particionamiento es inválida o falla la creación de grupo.
     */
    async register({
         topic, // <--- Parámetro renombrado (antes baseTopic)
         group,
         handler,
         partitioning = {},
         consumerIdBase,
         ...consumerOptions // Captura el resto de opciones
        }) {

        // Validación usando el parámetro 'topic' renombrado
        if (!topic || !group || typeof handler !== 'function') {
            // Mensaje de error ajustado
            throw new Error("Registro requiere 'topic', 'group' y una función 'handler'.");
        }

        // Determinar IDs de instancia y total (sin cambios)
        const instanceId = parseInt(partitioning.instanceId ?? process.env.INSTANCE_ID ?? '0', 10);
        const instanceCount = parseInt(partitioning.instanceCount ?? process.env.INSTANCE_COUNT ?? '1', 10);

        // Validar IDs (sin cambios)
        if (isNaN(instanceId) || isNaN(instanceCount) || instanceCount <= 0 || instanceId < 0 || instanceId >= instanceCount) {
             throw new Error(`Configuración de particionamiento inválida: instanceId=${instanceId}, instanceCount=${instanceCount}`);
        }

        // Calcular particiones asignadas (sin cambios)
        const assignedIndices = this.#getAssignedPartitionIndices(instanceId, instanceCount);

        if (assignedIndices.length === 0) {
             this.log.warn(`[Manager] No hay particiones asignadas a esta instancia (ID: ${instanceId}, Count: ${instanceCount}, Total Parts: ${this.totalPartitions}).`);
             return;
        }

        // Log usa 'topic' renombrado
        this.log.info(`[Manager] Instancia ${instanceId}/${instanceCount} asignada a particiones: [${assignedIndices.join(', ')}] para ${topic}/${group}`);

        // Iterar sobre cada partición asignada
        for (const index of assignedIndices) {
             // Usa 'topic' para generar nombre de stream y clave
            const partitionStream = this.partitioner.getPartitionStreamName(topic, index); // <-- Usa 'topic'
            const consumerId = `${consumerIdBase || `consumer-${group}`}-${process.pid}-${index}`;
            const consumerKey = `${partitionStream}:${group}:${consumerId}`;

            if (this.consumers.has(consumerKey)) {
                this.log.warn(`[Manager] Ya existe un consumidor para ${consumerKey}. Saltando.`);
                continue;
            }

            try {
                // Asegurar grupo en la partición usando 'topic'
                await this.#ensureGroupOnPartition(topic, group, index); // <-- Usa 'topic'

                // Crear instancia de Consumer (sin cambios aquí, recibe stream completo)
                const consumerInstance = new Consumer({
                    log: this.log,
                    topic: partitionStream, // <- Nombre completo del stream
                    group: group,
                    consumerId: consumerId,
                    ...consumerOptions
                });

                // Adjuntar handler (sin cambios aquí, pasa índice)
                consumerInstance.on('message', async (messageId, messageData, ack) => {
                    const handlerLog = this.log;
                    handlerLog.debug(`[Manager] Pasando msg ${messageId} de part. ${index} a handler`);
                    try {
                        await handler(messageId, messageData, handlerLog, index);
                        await ack();
                    } catch (processingError) {
                        handlerLog.error(`[Handler:${consumerId}] Error proc. msg ${messageId} part. ${index}:`, processingError);
                    }
                });

                // Adjuntar handler de errores (sin cambios)
                consumerInstance.on('error', (error, context) => {
                    this.log.error(`[ConsumerError:${consumerId}] Contexto [${context || 'general'}] Part ${index}:`, error.message);
                });

                // Iniciar consumidor (sin cambios)
                consumerInstance.start();

                // Almacenar instancia (sin cambios)
                this.consumers.set(consumerKey, consumerInstance);

            } catch (error) {
                 this.log.error(`[Manager] Falló registro para partición ${index} (${partitionStream}):`, error);
                 // Continuar con otras particiones por defecto
            }
        }
        this.log.info(`[Manager] Registro completado. ${this.consumers.size} consumidores de partición activos para esta instancia.`);
    }

    /**
     * Detiene todos los consumidores de particiones gestionados por esta instancia.
     * @returns {Promise<void>}
     */
    async stopAll() {
        // ... (lógica interna de stopAll sin cambios) ...
        this.log.info(`[Manager] Deteniendo ${this.consumers.size} consumidores de particiones...`);
        const stopPromises = [];
        let longestTimeout = 0;
        for (const [key, consumer] of this.consumers.entries()) {
            this.log.info(`[Manager] Solicitando detención para consumidor ${key}...`);
            consumer.stop();
            longestTimeout = Math.max(longestTimeout, consumer.blockTimeoutMs || 0);
        }
        const count = this.consumers.size;
        this.consumers.clear();
        this.log.info(`[Manager] Solicitud de detención enviada para ${count} consumidores.`);
        if (count > 0) {
             await new Promise(resolve => setTimeout(resolve, longestTimeout + 500));
             this.log.info('[Manager] Tiempo de espera para detención completado.');
        }
    }

    /**
    * Detiene un consumidor específico por su clave completa (topic:index:group:id).
    * @param {string} consumerKey - La clave completa del consumidor.
    * @returns {Promise<void>}
    */
    async stop(consumerKey) {
         // ... (lógica interna de stop(key) sin cambios) ...
         if (this.consumers.has(consumerKey)) {
            this.log.info(`[Manager] Deteniendo consumidor específico ${consumerKey}...`);
            const consumer = this.consumers.get(consumerKey);
            const timeout = consumer.blockTimeoutMs || 2000;
            consumer.stop();
            this.consumers.delete(consumerKey);
            await new Promise(resolve => setTimeout(resolve, timeout + 500));
            this.log.info(`[Manager] Consumidor ${consumerKey} detenido.`);
        } else {
            this.log.warn(`[Manager] No se encontró consumidor con clave ${consumerKey} para detener.`);
        }
    }
}

export default ConsumerManager;