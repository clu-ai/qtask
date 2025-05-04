import { getClient } from './RedisConnection.js';

class Publisher {
    constructor({ log }) {
        if (!log) {
            throw new Error("Publisher requiere una instancia de 'log'.");
        }
        this.log = log;
    }

    async publish(topic, messageData, options = {}) {
        if (!topic || messageData === undefined || messageData === null) {
            this.log.error('[Publisher] Se requiere "topic" y "messageData" para publicar.');
            return null;
        }

        const messageIdToUse = options.id || '*';
        let fieldsToSave;

        try {
            const redis = getClient();

            // --- Log de Depuración 1 ---
            this.log.debug(`[Publisher-DEBUG] Recibido para publicar en '${topic}':`, messageData);
            // --- Fin Log 1 ---

            if (typeof messageData === 'object' && !Array.isArray(messageData)) {
                this.log.debug(`[Publisher-DEBUG] Procesando como objeto.`);

                fieldsToSave = Object.entries(messageData).flat();

                // --- Log de Depuración 2 ---
                this.log.debug(`[Publisher-DEBUG] Resultado de Object.entries().flat():`, fieldsToSave);
                // --- Fin Log 2 ---

                if (fieldsToSave.some(item => item === undefined || item === null)) {
                     this.log.warn(`[Publisher] messageData objeto contenía undefined/null, publicando como string: ${JSON.stringify(messageData)}`);
                     fieldsToSave = ['message', JSON.stringify(messageData)];
                } else if (fieldsToSave.length === 0) { // La condición problemática
                     // --- Log de Depuración 3 ---
                     this.log.warn(`[Publisher-DEBUG] ¡¡¡ fieldsToSave.length es 0 !!! El messageData original fue:`, messageData);
                     // --- Fin Log 3 ---
                     this.log.warn(`[Publisher] messageData objeto resultó vacío, publicando placeholder.`);
                     fieldsToSave = ['_placeholder', 'empty_object'];
                }
                 // Si no es ninguno de los anteriores, fieldsToSave ya tiene el valor correcto aplanado.

            } else {
                this.log.debug(`[Publisher-DEBUG] Procesando como NO-objeto (o array).`);
                fieldsToSave = ['message', typeof messageData === 'string' ? messageData : JSON.stringify(messageData)];
            }

             // --- Log de Depuración 4 ---
             this.log.debug(`[Publisher-DEBUG] Campos finales para XADD:`, fieldsToSave);
             // --- Fin Log 4 ---

            const publishedId = await redis.xadd(topic, messageIdToUse, ...fieldsToSave);

            this.log.info(`[Publisher] Mensaje publicado en '${topic}' con ID: ${publishedId}`);
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