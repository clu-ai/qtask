import { getClient } from './RedisConnection.js';

class Publisher {
    /**
     * @param {object} options
     * @param {Logging} options.log - Instancia del logger.
     * @param {Partitioner} options.partitioner - Instancia del Partitioner.
     */
    constructor({ log, partitioner }) {
        if (!log || !partitioner) {
            throw new Error("Publisher requiere instancias de 'log' y 'partitioner'.");
        }
        this.log = log;
        this.partitioner = partitioner;
    }

    /**
     * Publica un mensaje en la partición correcta de un stream de Redis.
     * @param {string} baseTopic - El nombre base del topic (ej: 'WHATSAPP').
     * @param {string|number} partitionKey - La clave para determinar la partición.
     * @param {object|string} messageData - Los datos a publicar.
     * @param {object} [options={}] - Opciones adicionales para XADD (ej: { id: '...' }).
     * @returns {Promise<string|null>} - ID del mensaje o null si falla.
     */
    async publish(baseTopic, partitionKey, messageData, options = {}) {
        if (!baseTopic || partitionKey === undefined || partitionKey === null || messageData === undefined || messageData === null) {
            this.log.error('[Publisher] Se requiere "baseTopic", "partitionKey" y "messageData".');
            return null;
        }

        const messageIdToUse = options.id || '*';
        let fieldsToSave;

        try {
            // 1. Calcular Partición
            const partitionIndex = this.partitioner.getPartition(partitionKey);
            const targetStream = this.partitioner.getPartitionStreamName(baseTopic, partitionIndex);

            this.log.debug(`[Publisher] Clave '${partitionKey}' mapeada a partición ${partitionIndex} (Stream: ${targetStream})`);

            // 2. Preparar Campos (lógica anterior sin cambios)
            const messageIdToUse = options.id || '*';
            let fieldsToSave;
            if (typeof messageData === 'object' && !Array.isArray(messageData)) {
                // ... (lógica para aplanar objeto) ...
                fieldsToSave = Object.entries(messageData).flat();
                if (fieldsToSave.some(item => item === undefined || item === null)) { /*...*/ fieldsToSave = ['message', JSON.stringify(messageData)]; }
                else if (fieldsToSave.length === 0) { /*...*/ fieldsToSave = ['_placeholder', 'empty_object']; }
            } else {
                fieldsToSave = ['message', typeof messageData === 'string' ? messageData : JSON.stringify(messageData)];
            }

            this.log.debug(`[Publisher] Publicando en '${targetStream}' ID '${messageIdToUse}', Campos:`, fieldsToSave);

            // 3. Ejecutar XADD en la partición correcta
            const redis = getClient();
            const publishedId = await redis.xadd(targetStream, messageIdToUse, ...fieldsToSave);

            this.log.info(`[Publisher] Mensaje publicado en '${targetStream}' con ID: ${publishedId}`);
            return publishedId;

        } catch (error) {
            // ... (manejo de errores sin cambios) ...
            if (error.message.includes('[RedisConnection]')) {
                this.log.error(`[Publisher] No se pudo publicar - Redis no conectado.`);
            } else {
                this.log.error(`[Publisher] Error al publicar mensaje en '${topic}':`, error);
            }
            return null;
        }
    }
}

export default Publisher;