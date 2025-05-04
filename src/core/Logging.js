import util from 'util'; // Para formatear mensajes como console.log

/**
 * Define los niveles de log estándar y su severidad numérica.
 * Menor número = más detallado, Mayor número = más crítico.
 */
const LOG_LEVELS = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
  silent: 4, // Para deshabilitar todos los logs
};

/**
 * Colores ANSI para la consola (opcional pero útil).
 */
const COLORS = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  gray: '\x1b[90m',
};

/**
 * @class Logging
 * Proporciona una interfaz para registrar mensajes con niveles y formato personalizables.
 */
class Logging {
  /**
   * Crea una instancia del logger.
   * @param {object} options - Opciones de configuración.
   * @param {string} [options.serviceName='App'] - Nombre del servicio/aplicación para identificar los logs.
   * @param {'debug' | 'info' | 'warn' | 'error' | 'silent'} [options.minLevel='info'] - Nivel mínimo de log a mostrar.
   * @param {boolean} [options.useColors=true] - Si se deben usar colores en la salida de consola.
   * @param {string} [options.timestampFormat='iso'] - Formato del timestamp ('iso', 'local', 'none').
   */
  constructor(options = {}) {
    this.serviceName = options.serviceName || 'App';

    // Validar y establecer el nivel mínimo
    const providedLevel = options.minLevel?.toLowerCase();
    if (providedLevel && providedLevel in LOG_LEVELS) {
      this.minLevel = providedLevel;
    } else {
      this.minLevel = 'info';
      if (providedLevel) {
        this._logDirect('warn', `Nivel de log inválido '${options.minLevel}'. Usando '${this.minLevel}'.`);
      }
    }
    this.minLevelSeverity = LOG_LEVELS[this.minLevel];

    this.useColors = options.useColors !== false; // Habilitado por defecto
    this.timestampFormat = options.timestampFormat || 'iso'; // 'iso', 'local', 'none'
  }

  /**
   * Obtiene el timestamp formateado según la configuración.
   * @returns {string} Timestamp formateado o string vacío.
   */
  _getTimestamp() {
    if (this.timestampFormat === 'none') {
      return '';
    }
    const now = new Date();
    if (this.timestampFormat === 'local') {
      return now.toLocaleString(); // Formato local legible
    }
    // Por defecto (o si es 'iso')
    return now.toISOString(); // Formato ISO 8601
  }

  /**
   * Formatea el mensaje completo del log.
   * @param {string} level - Nivel del log (e.g., 'info').
   * @param {any[]} args - Argumentos a loguear (similar a console.log).
   * @returns {string} Mensaje formateado.
   */
  _formatMessage(level, args) {
    const timestamp = this._getTimestamp();
    const levelUpper = level.toUpperCase();
    // Usa util.format para manejar múltiples argumentos y tipos como lo hace console.log
    const messageContent = util.format(...args);

    // Formato base: [Timestamp] [LEVEL] [ServiceName] Mensaje
    return `${timestamp ? `[${timestamp}] ` : ''}[${levelUpper}] [${this.serviceName}] ${messageContent}`;
  }

  /**
   * Método central privado para registrar el mensaje si el nivel es adecuado.
   * @param {'debug' | 'info' | 'warn' | 'error'} level - Nivel del mensaje actual.
   * @param {any[]} args - Argumentos a loguear.
   */
  _log(level, args) {
    const currentLevelSeverity = LOG_LEVELS[level];

    // No loguear si el nivel actual es menor que el mínimo configurado
    if (currentLevelSeverity < this.minLevelSeverity) {
      return;
    }

    const formattedMessage = this._formatMessage(level, args);
    this._logDirect(level, formattedMessage);
  }

  /**
   * Escribe directamente a la consola, aplicando colores si están habilitados.
   * Este método no revisa el nivel mínimo, asume que ya fue validado.
   * @param {string} level - Nivel del log para determinar color y método de consola.
   * @param {string} message - Mensaje ya formateado.
   */
   _logDirect(level, message) {
    let color = COLORS.reset;
    let consoleMethod = 'log'; // Método de consola a usar

    if (this.useColors) {
        switch (level) {
            case 'debug':
                color = COLORS.gray;
                consoleMethod = 'debug'; // O 'log', dependiendo de la visibilidad deseada
                break;
            case 'info':
                color = COLORS.cyan;
                consoleMethod = 'info';
                break;
            case 'warn':
                color = COLORS.yellow;
                consoleMethod = 'warn';
                break;
            case 'error':
                color = COLORS.red;
                consoleMethod = 'error';
                break;
        }
    } else {
         // Asignar método de consola sin colores
         switch (level) {
            case 'debug': consoleMethod = 'debug'; break;
            case 'info': consoleMethod = 'info'; break;
            case 'warn': consoleMethod = 'warn'; break;
            case 'error': consoleMethod = 'error'; break;
        }
    }

    // Si el método específico no existe (como 'debug' en algunos entornos), usa 'log'
    const methodToCall = console[consoleMethod] || console.log;

    if (this.useColors) {
        methodToCall(`${color}${message}${COLORS.reset}`);
    } else {
        methodToCall(message);
    }
   }

  // --- Métodos Públicos de Logging ---

  /**
   * Registra un mensaje con nivel DEBUG.
   * @param {...any} args - Mensaje y argumentos opcionales a loguear.
   */
  debug(...args) {
    this._log('debug', args);
  }

  /**
   * Registra un mensaje con nivel INFO.
   * @param {...any} args - Mensaje y argumentos opcionales a loguear.
   */
  info(...args) {
    this._log('info', args);
  }

  /**
   * Registra un mensaje con nivel WARN.
   * @param {...any} args - Mensaje y argumentos opcionales a loguear.
   */
  warn(...args) {
    this._log('warn', args);
  }

  /**
   * Registra un mensaje con nivel ERROR.
   * @param {...any} args - Mensaje y argumentos opcionales a loguear.
   */
  error(...args) {
    this._log('error', args);
  }

  /**
   * Cambia el nivel mínimo de log dinámicamente.
   * @param {'debug' | 'info' | 'warn' | 'error' | 'silent'} newLevel - El nuevo nivel mínimo.
   */
  setLevel(newLevel) {
     const level = newLevel?.toLowerCase();
     if (level && level in LOG_LEVELS) {
      this.minLevel = level;
      this.minLevelSeverity = LOG_LEVELS[this.minLevel];
      this.info(`Nivel de log cambiado a: ${this.minLevel}`);
     } else {
      this.warn(`Intento de cambiar a nivel de log inválido: ${newLevel}`);
     }
  }
}

export default Logging; // Exporta la clase para usarla con import