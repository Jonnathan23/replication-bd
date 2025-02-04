import { Pool } from 'pg';
import cron from 'node-cron';
import colors from 'colors';

// Configuración de conexiones a ambas bases de datos
const primaryDB = new Pool({
    user: 'postgres',
    host: '192.168.72.129', // IP de la base de datos primaria
    database: 'tolerancia_db',
    password: '0000',
    port: 5432,
});

const secondaryDB = new Pool({
    user: 'postgres',
    host: '192.168.72.133', // IP de la base de datos secundaria
    database: 'tolerancia_db',
    password: '0000',
    port: 5432,
});

interface Change {
    id: number;
    metodo: string;
    tabla: string;
    descripcion: any;
    fecha: string;
}

// Función para verificar si la base de datos está disponible
async function checkDatabaseAvailability(db: Pool, dbName: string): Promise<boolean> {
    try {
        const client = await db.connect();
        client.release();
        return true;
    } catch (error) {
        console.error(colors.bgRed(`Máquina ${dbName} no se encuentra disponible.`));
        return false;
    }
}

// Función para construir la consulta SQL
function buildQuery(change: Change): string {
    let data;
    try {
        data = typeof change.descripcion === 'object' ? change.descripcion : JSON.parse(change.descripcion);
    } catch (error: any) {
        console.error(colors.bgRed(`Error al parsear JSON en ${change.tabla}: ${error.message}`));
        return '';
    }

    let query = '';
    if (change.metodo === 'INSERT') {
        const columns = Object.keys(data).join(', ');
        const values = Object.values(data).map(value => `'${value}'`).join(', ');
        query = `INSERT INTO ${change.tabla} (${columns}) VALUES (${values});`;
    } else if (change.metodo === 'UPDATE') {
        const setValues = Object.entries(data)
            .map(([key, value]) => `${key} = '${value}'`)
            .join(', ');
        query = `UPDATE ${change.tabla} SET ${setValues} WHERE id = '${data.id}';`;
    } else if (change.metodo === 'DELETE') {
        query = `DELETE FROM ${change.tabla} WHERE id = '${data.id}';`;
    }
    return query;
}

// Función para sincronizar los cambios
async function syncChanges(sourceDB: Pool, targetDB: Pool, sourceName: string, targetName: string): Promise<void> {
    if (!(await checkDatabaseAvailability(sourceDB, sourceName))) return;
    if (!(await checkDatabaseAvailability(targetDB, targetName))) return;

    try {
        console.log(colors.cyan.bold(`Sincronizando desde ${sourceName} a ${targetName}...`));
        const client = await sourceDB.connect();
        try {
            const result = await client.query<Change>('SELECT * FROM "database_changes" ORDER BY id ASC');

            if (result.rows.length === 0) {
                console.log(colors.gray(`Sin cambios en ${sourceName}.`));
                return;
            }

            for (const row of result.rows) {
                const sqlQuery = buildQuery(row);
                if (!sqlQuery) {
                    console.log(colors.red(`Saltando fila con error en JSON en ${sourceName}`));
                    continue;
                }
                console.log(colors.yellow(`Ejecutando en ${targetName}:`), sqlQuery);
                await targetDB.query(sqlQuery);
                await sourceDB.query('DELETE FROM "database_changes" WHERE id = $1', [row.id]);
            }
            console.log(colors.green.bold(`Sincronización de ${sourceName} a ${targetName} completa.`));
        } catch (error: any) {
            console.error(colors.bgRed(`Error sincronizando de ${sourceName} a ${targetName}: ${error.message}`));
        } finally {
            client.release();
        }
    } catch (error: any) {
        console.error(colors.bgRed(`Error conectando a ${sourceName}: ${error.message}`));
    }
}

// Ejecutar la sincronización cada 10 segundos
cron.schedule('*/10 * * * * *', async () => {
    console.log(colors.blue('Iniciando sincronización...'));
    await syncChanges(primaryDB, secondaryDB, 'PrimaryDB', 'SecondaryDB');
    await syncChanges(secondaryDB, primaryDB, 'SecondaryDB', 'PrimaryDB');
    console.log(colors.magenta('Sincronización finalizada.'));
});
