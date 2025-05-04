import dotenv from 'dotenv';
import path from 'path';

// Cargar el archivo .env correcto dependiendo del entorno
const envFile = process.env.NODE_ENV === 'production' ? '.env.production' : '.env.development';

// Configurar dotenv para cargar las variables del archivo correspondiente
dotenv.config({ path: path.resolve(process.cwd(), envFile) });

// Exportar las variables de entorno
const config = {
  REDIS_HOST: process.env.REDIS_HOST,
  REDIS_USERNAME: process.env.REDIS_USERNAME,
  REDIS_PASSWORD: process.env.REDIS_PASSWORD,
  REDIS_PORT: process.env.REDIS_PORT,
};

export default config;