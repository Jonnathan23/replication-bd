import { Pool } from 'pg';
import cron from 'node-cron';
import colors from 'colors';

// Configuración de conexiones a ambas bases de datos
const primaryDB = new Pool({
    user: 'postgres',
    host: '192.168.72.129',
    database: 'tolerancia_db',
    password: '0000',
    port: 5432,
});

const secondaryDB = new Pool({
    user: 'postgres',
    host: '192.168.72.133',
    database: 'tolerancia_db',
    password: '0000',
    port: 5432,
});

interface Change {
    id: number;
    query: string;
}

// Función para sincronizar los cambios
async function syncChanges(sourceDB: Pool, targetDB: Pool, sourceName: string, targetName: string): Promise<void> {
    try {
        console.log(colors.cyan.bold(`Sincronizando desde ${sourceName} a ${targetName}...`));
        const client = await sourceDB.connect();
        const result = await client.query<Change>('SELECT * FROM "database_changes" ORDER BY id ASC');

        if (result.rows.length === 0) {
            console.log(`Sin cambios en ${sourceName}.`);
            client.release();
            return;
        }
        
        for (const row of result.rows) {
            console.log(colors.yellow(`Ejecutando en ${targetName}:`), row.query);
            await targetDB.query(row.query);
            await sourceDB.query('DELETE FROM "Changes" WHERE id = $1', [row.id]);
        }
        client.release();
        console.log(colors.green.bold(`Sincronización de ${sourceName} a ${targetName} completa.`));
    } catch (error) {
        console.error(colors.bgRed(`Error sincronizando de ${sourceName} a ${targetName}:`), error);
    }
}

// Ejecutar la sincronización cada 10 segundos
cron.schedule('*/10 * * * * *', async () => {
    console.log(colors.blue('Iniciando sincronización...'));
    await syncChanges(primaryDB, secondaryDB, 'PrimaryDB', 'SecondaryDB');
    await syncChanges(secondaryDB, primaryDB, 'SecondaryDB', 'PrimaryDB');
    console.log(colors.america('Sincronización finalizada.'));
});