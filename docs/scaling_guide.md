# Guía de Escalado para el Sistema QTask

## Introducción

A medida que la carga de trabajo de tu sistema aumenta (más tareas/mensajes a procesar), necesitas una forma de escalar la capacidad de tus workers (consumidores) para mantener un buen rendimiento y prevenir cuellos de botella.

Este sistema utiliza Redis Streams particionados. Un concepto clave es que **el procesamiento de mensajes *dentro* de una única partición es secuencial**, realizado por el único consumidor asignado a ella en un momento dado (aunque la recuperación de fallos puede reasignarlo). Por lo tanto, para aumentar el procesamiento paralelo general, necesitamos distribuir el trabajo a través de *múltiples particiones* y tener *múltiples workers* procesándolas concurrentemente.

## Concepto Central: Escalado Horizontal vía Particionamiento

La estrategia principal para escalar este sistema es el **escalado horizontal basado en particiones**. Funciona así:

1.  **Dividir Trabajo:** La cola lógica de mensajes (topic) se divide en múltiples "canales" o **particiones** separadas. Cada partición es un Redis Stream independiente (ej., `WHATSAPP:0`, `WHATSAPP:1`, ... `WHATSAPP:N-1`, donde `N` es `TOTAL_PARTITIONS`).
2.  **Distribuir Mensajes:** Cada nuevo mensaje se asigna a una partición específica usando una **clave de partición** (`partitionKey`, ej., el número de teléfono del destinatario) y una función de hash/módulo (gestionada por la clase `Partitioner`). Idealmente, diferentes claves se distribuyen entre diferentes particiones.
3.  **Procesamiento Concurrente:** Múltiples **workers** (instancias de tu aplicación Node.js que ejecutan `QTask`, típicamente corriendo en **Pods** separados si usas Kubernetes) se ejecutan en paralelo. Cada worker adquiere automáticamente la responsabilidad de procesar un subconjunto de las particiones disponibles.
4.  **Paralelismo Incrementado:** Al aumentar el número total de particiones (`TOTAL_PARTITIONS`) y el número de workers (Pods), más mensajes (con diferentes `partitionKey`) pueden ser procesados simultáneamente por diferentes workers en diferentes particiones.

## Estrategia Recomendada: Más Particiones, Más Workers (Pods)

La forma más efectiva y resiliente de escalar es **aumentar el número total de particiones y ejecutar más instancias de workers (Pods), asegurando que la carga se distribuya entre ellos.**

* **Objetivo:** Maximizar la concurrencia distribuyendo la carga entre muchas unidades de procesamiento independientes y minimizar el impacto si una unidad falla.
* **Mecanismo:**
    * Incrementar el número total de particiones (`TOTAL_PARTITIONS` en la configuración de `QTask`).
    * Ejecutar un número suficiente de instancias de la aplicación (Pods).
    * **Configurar cada instancia (Pod) con un `INSTANCE_ID` único y el `INSTANCE_COUNT` total** (usando variables de entorno).
    * El `ConsumerManager` dentro de cada instancia `QTask` usará estas variables para calcular automáticamente qué particiones le corresponden procesar, usando la fórmula: `partition_index % INSTANCE_COUNT === INSTANCE_ID`.
* **Analogía:** Piensa en un supermercado. En lugar de tener 2 cajas súper rápidas (pocos workers manejando muchas particiones cada uno), es mejor tener muchas cajas estándar (muchos workers, cada uno responsable de un subconjunto de particiones). Si una caja estándar cierra, el impacto general en la cola es menor.

## Configuración y Despliegue Clave

Las variables más importantes para este enfoque de escalado son:

* `TOTAL_PARTITIONS`:
    * **Qué es:** El número total de particiones (Redis Streams `topic:index`) en las que se dividirá el topic lógico. Se pasa al constructor de `QTask`.
    * **Impacto:** Define el **nivel máximo posible de paralelismo**. Si tienes 16 particiones, como máximo 16 consumidores (posiblemente en diferentes Pods) pueden estar procesando mensajes diferentes concurrentemente.
    * **Consideración:** Elige un número basado en tu carga esperada y paralelismo deseado. Es mejor sobrestimar ligeramente que subestimar significativamente. Cambiar este número en producción requiere planificación (migración de datos o posible pérdida de mensajes en streams antiguos si no se maneja con cuidado). Valores como 16, 32, 64, 128 son comunes.

* `INSTANCE_COUNT` (Variable de Entorno):
    * **Qué es:** El número **total** de instancias/Pods que *planeas* ejecutar para procesar este topic/grupo. **Debe ser el mismo valor para todos los Pods** que participan en el grupo.
    * **Impacto:** Determina cómo se dividen las `TOTAL_PARTITIONS` entre los workers. Afecta la carga por Pod y la resiliencia.
    * **Ejemplo:** Si `TOTAL_PARTITIONS=16` y `INSTANCE_COUNT=4`, cada Pod manejará aproximadamente 4 particiones.

* `INSTANCE_ID` (Variable de Entorno):
    * **Qué es:** Un identificador **único** para cada instancia/Pod, comenzando desde `0` hasta `INSTANCE_COUNT - 1`.
    * **Impacto:** Determina *cuál* subconjunto de particiones manejará este Pod específico (`partition_index % INSTANCE_COUNT === INSTANCE_ID`).
    * **Orquestación:** Sistemas como Kubernetes pueden inyectar esto automáticamente. Por ejemplo, en un `StatefulSet` con 4 réplicas, los Pods se llaman `pod-0`, `pod-1`, `pod-2`, `pod-3`. Puedes extraer el índice (`0`, `1`, `2`, `3`) del nombre del Pod y usarlo como `INSTANCE_ID` (asegurándote que `INSTANCE_COUNT` esté configurado como `4` en todas las réplicas).

## Ejemplos de Despliegue (Conceptual)

Asumiendo que cada instancia de tu aplicación Node.js corre en un Pod y lee `INSTANCE_ID` e `INSTANCE_COUNT` del entorno:

**Escenario 1: Carga Moderada**

* Quieres procesar hasta 16 particiones en paralelo.
* Decides correr 8 Pods para buena resiliencia (cada uno manejaría 2 particiones).
* **Configuración QTask:**
    * `TOTAL_PARTITIONS = 16`
* **Configuración Despliegue (Pods):**
    * Número de Réplicas/Pods = 8
    * Variable Entorno `INSTANCE_COUNT = 8` (en *todos* los Pods)
    * Variable Entorno `INSTANCE_ID` = 0, 1, 2, 3, 4, 5, 6, 7 (único para cada Pod)

**Escenario 2: Carga Alta**

* Necesitas alto paralelismo, digamos 64 particiones.
* Correrás 16 Pods (cada uno manejaría 4 particiones).
* **Configuración QTask:**
    * `TOTAL_PARTITIONS = 64`
* **Configuración Despliegue (Pods):**
    * Número de Réplicas/Pods = 16
    * Variable Entorno `INSTANCE_COUNT = 16` (en *todos* los Pods)
    * Variable Entorno `INSTANCE_ID` = 0, 1, ..., 15 (único para cada Pod)

**Escenario 3: Baja Carga / Granularidad Máxima**

* Solo necesitas paralelismo básico de 8 particiones.
* Quieres máxima granularidad, 1 partición por Pod.
* **Configuración QTask:**
    * `TOTAL_PARTITIONS = 8`
* **Configuración Despliegue (Pods):**
    * Número de Réplicas/Pods = 8
    * Variable Entorno `INSTANCE_COUNT = 8` (en *todos* los Pods)
    * Variable Entorno `INSTANCE_ID` = 0, 1, ..., 7 (único para cada Pod)

## Beneficios de esta Estrategia

* **Mayor Rendimiento (Throughput):** Más mensajes procesados por unidad de tiempo debido a la ejecución paralela.
* **Mejor Resiliencia:** El fallo de un Pod/worker solo afecta temporalmente a las particiones que tenía asignadas. La lógica `XAUTOCLAIM` permite que otros workers (o el mismo worker al reiniciar) reclamen y procesen los mensajes pendientes después de `minIdleTimeMs`.
* **Mejor Utilización de Recursos:** La carga de CPU/Memoria/Red se distribuye entre múltiples máquinas/Pods.
* **Escalabilidad Flexible:** Fácil escalar hacia arriba o abajo ajustando el número de Pods/workers y la variable `INSTANCE_COUNT` (requiere un redespliegue o rolling update), siempre que `TOTAL_PARTITIONS` sea suficientemente alto.

## Consideraciones Adicionales

* **Elegir `TOTAL_PARTITIONS`:** Decisión importante. Un número mayor permite más escalabilidad futura pero añade una pequeña sobrecarga en Redis. Es difícil cambiarlo sin impacto una vez en producción. Analiza tu carga máxima esperada.
* **Configuración `INSTANCE_ID` / `INSTANCE_COUNT`:** Es **crítico** que `INSTANCE_COUNT` sea consistente en todos los Pods activos y que `INSTANCE_ID` sea único para cada uno (0 a N-1). Errores aquí llevarán a que algunas particiones no sean procesadas o sean procesadas por múltiples workers (aunque el grupo Redis previene el procesamiento duplicado del mismo mensaje *nuevo*).
* **Orquestación:** Necesitarás una forma de ejecutar y gestionar las múltiples instancias (Kubernetes es ideal, especialmente `StatefulSet` para IDs estables, pero Docker Swarm, systemd, supervisord también son opciones).
* **Capacidad de Redis:** Asegúrate que tu instancia de Redis pueda manejar el número incrementado de streams, grupos, y conexiones/comandos de tus workers.
* **Distribución de Claves:** Si pocas `partitionKey` reciben la mayoría de los mensajes ("hot keys"), esas particiones específicas pueden volverse cuellos de botella. Tener un mayor número de `TOTAL_PARTITIONS` ayuda a mitigar este efecto.
* **Manejo de Fallos:** La recuperación se basa en `XAUTOCLAIM`. Ajusta `minIdleTimeMs` (en la configuración del `Consumer` / `QTask.register`) a un valor razonable (ej. 30 segundos, 1 minuto, 5 minutos) según cuánto tiempo puedes esperar para reprocesar un mensaje de un worker caído.

## Rebalanceo Dinámico vs. Estático

La estrategia descrita aquí usa **asignación estática** (cada pod calcula sus particiones al inicio basado en ID/Count). No implementa rebalanceo dinámico automático como Kafka (donde los pods se coordinarían entre sí para redistribuir particiones activamente sin reiniciar si uno muere). El rebalanceo dinámico es posible pero **significativamente más complejo** de implementar sobre Redis. La combinación de asignación estática + `XAUTOCLAIM` + gestión por orquestador suele ser suficiente y más fácil de mantener.