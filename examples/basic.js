import QTask from '../src/index.js';

// Definir TOPIC y GROUP (pueden venir de config o estar aquí)
const WHATSAPP_TOPIC = 'WHATSAPP'; // Ejemplo usando config
const WHATSAPP_GROUP = 'fazpi-whatsapp';

// Define tus handlers específicos aquí
async function handleWhatsappMessage(id, data, handlerLog) {
    handlerLog.info(`Procesando mensaje WhatsApp ${id}:`, data);
}

// Función principal asíncrona
async function main() {
    let qtaskInstance = null; // Para poder usarla en shutdown

    try {
        // 1. Crear instancia de QTask con configuración
        qtaskInstance = new QTask({
            REDIS_HOST: "40.76.248.127",
            REDIS_PORT: 30000,
            REDIS_USERNAME: "default",
            REDIS_PASSWORD: "3n7F8fqEFfzC2vfCaeztlXNKxtcIJjxX2Mht+GadPmyGdXbrI36CHw==",
            logLevel: process.env.NODE_ENV === 'development' ? 'debug' : 'info', // Nivel de log según entorno
            logServiceName: 'WhatsAppService' // Nombre específico del servicio
            // redisOptions: { enableOfflineQueue: false } // Ejemplo opción ioredis
        });

        // 2. Conectar a Redis e inicializar componentes internos
        await qtaskInstance.connect(); // Espera a que conecte

        // 3. Registrar los handlers necesarios
        await qtaskInstance.register({
            topic: WHATSAPP_TOPIC,
            group: WHATSAPP_GROUP,
            handler: handleWhatsappMessage
        });

        qtaskInstance.publish(WHATSAPP_TOPIC, {
            to: '3205104418',
            message: 'Hola, este es un mensaje de prueba'
        });

        qtaskInstance.log.info("--- Aplicación QTask inicializada y corriendo ---");

    } catch (error) {
        console.error("Error fatal durante la inicialización de QTask:", error);
        if (qtaskInstance && typeof qtaskInstance.stop === 'function') {
            await qtaskInstance.stop().catch(e => console.error("Error durante stop:", e));
        } else {
            // YA NO llamamos a disconnectRedis aquí, porque si no hay instancia, no hubo conexión exitosa.
            console.log("[Catch Main] No se pudo inicializar QTask, no hay conexión activa que cerrar.");
        }
        process.exit(1);
    }

    // 5. Manejo de cierre grácil
    const shutdown = async (signal) => {
        console.log(`\nRecibido ${signal}. Iniciando cierre grácil...`);
        if (qtaskInstance && typeof qtaskInstance.stop === 'function') {
            await qtaskInstance.stop();
        } else {
            // YA NO llamamos a disconnectRedis aquí.
            console.log("[Shutdown] No hay instancia QTask activa que detener.");
        }
        console.log('Cierre completado. Adiós.');
        process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
}

// Ejecutar main
main().catch(async (err) => { // async para el await
    console.error("Error crítico inesperado en la ejecución principal:", err);
    if (qtaskInstance && typeof qtaskInstance.stop === 'function') {
        await qtaskInstance.stop().catch(e => console.error("Error durante stop en catch final:", e));
    } else {
        // YA NO llamamos a disconnectRedis aquí.
        console.log("[Catch Final] No hay instancia QTask activa que detener.");
    }
    process.exit(1);
});