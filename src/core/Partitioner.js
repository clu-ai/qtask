// Función de hash simple (puedes reemplazarla por una más robusta si es necesario, ej: FNV1a, Murmur)
// Esta convierte el string en un número sumando los códigos de caracteres.
function simpleStringHash(str) {
    let hash = 0;
    if (!str || str.length === 0) {
        return hash;
    }
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char; // Algoritmo simple (similar a Java String.hashCode)
        hash |= 0; // Convertir a entero de 32bit
    }
    // Asegurarse de que el resultado sea no negativo antes del módulo
    return Math.abs(hash);
}

class Partitioner {
    /**
     * @param {number} totalPartitions - El número total de particiones disponibles.
     */
    constructor(totalPartitions) {
        if (!Number.isInteger(totalPartitions) || totalPartitions <= 0) {
            throw new Error("Partitioner requiere un 'totalPartitions' entero y positivo.");
        }
        this.totalPartitions = totalPartitions;
    }

    /**
     * Calcula el índice de partición para una clave dada.
     * @param {string|number} partitionKey - La clave para determinar la partición.
     * @returns {number} - El índice de la partición (0 a totalPartitions - 1).
     */
    getPartition(partitionKey) {
        if (partitionKey === undefined || partitionKey === null) {
            // Si no hay clave, asigna aleatoriamente o a una partición por defecto (ej: 0)
            // Asignar aleatorio puede distribuir mejor la carga sin clave.
            return Math.floor(Math.random() * this.totalPartitions);
        }

        const keyString = String(partitionKey); // Asegura que sea string para el hash
        const hashValue = simpleStringHash(keyString);
        const partitionIndex = hashValue % this.totalPartitions;

        return partitionIndex;
    }

    /**
     * Construye el nombre completo del stream para una partición específica.
     * @param {string} baseTopic - El nombre base del topic (ej: 'WHATSAPP').
     * @param {number} partitionIndex - El índice de la partición.
     * @returns {string} - El nombre completo del stream (ej: 'WHATSAPP:0').
     */
    getPartitionStreamName(baseTopic, partitionIndex) {
        if (partitionIndex < 0 || partitionIndex >= this.totalPartitions) {
             throw new Error(`Índice de partición ${partitionIndex} fuera de rango (0-${this.totalPartitions - 1}).`);
        }
        return `${baseTopic}:${partitionIndex}`;
    }
}

export default Partitioner;