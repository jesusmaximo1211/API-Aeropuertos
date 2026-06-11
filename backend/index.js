const express = require('express');
const mongoose = require('mongoose');
const Redis = require('ioredis');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

// Conexiones
const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/airport_db';
mongoose.connect(mongoUrl);
mongoose.connection.on('connected', () => console.log('Mongoose conectado a MongoDB.'));
mongoose.connection.on('error', (err) => console.error('Error de conexión en Mongoose:', err));

const redisGeo = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379')
});
const redisPop = new Redis({
    host: process.env.REDIS_POP_HOST || 'localhost',
    port: parseInt(process.env.REDIS_POP_PORT || '6381')
});

// Esquema de Mongoose
const Airport = mongoose.model('Airport', new mongoose.Schema({
    name: String,
    city: String,
    iata_faa: String,
    icao: String,
    lat: Number,
    lng: Number,
    alt: Number,
    tz: String
}, { collection: 'airports' }));

// --- ETAPA 2.5: Sembrado Automático de Base de Datos ---
async function seedDatabase() {
    const count = await Airport.countDocuments();
    if (count === 0) {
        console.log("MongoDB está vacío. Iniciando sembrado (seeding) desde data_trasport.json...");
        try {
            let jsonPath = path.join(__dirname, 'data_trasport.json');
            if (!fs.existsSync(jsonPath)) {
                jsonPath = path.join(__dirname, '..', 'data_trasport.json');
            }
            
            if (fs.existsSync(jsonPath)) {
                const rawData = fs.readFileSync(jsonPath, 'utf8');
                const formattedData = '[' + rawData.trim().replace(/\}\s*\{/g, '},{') + ']';
                const airports = JSON.parse(formattedData);
                
                await Airport.insertMany(airports);
                console.log(`Sembrado completado. Se insertaron ${airports.length} aeropuertos.`);
            } else {
                console.log("No se encontró el archivo data_trasport.json para sembrar la base de datos.");
            }
        } catch (err) {
            console.error("Error al sembrar la base de datos:", err);
        }
    }
}

// --- ETAPA 3: Sincronización Automática al Arrancar ---
async function syncToRedis() {
    const count = await redisGeo.zcard('airports-geo');
    if (count === 0) { // Solo sincronizar si Redis está vacío
        const airports = await Airport.find({});
        const pipeline = redisGeo.pipeline();
        airports.forEach(ap => {
            if (ap.iata_faa && ap.lat && ap.lng) {
                pipeline.geoadd('airports-geo', ap.lng, ap.lat, ap.iata_faa);
            }
        });
        await pipeline.exec();
        console.log("Redis sincronizado correctamente.");
    }
}

async function initializeApp() {
    await seedDatabase();
    await syncToRedis();
}
initializeApp();


// --- ENDPOINTS ---

// GET /airports (Lista completa para Leaflet)
app.get('/airports', async (req, res) => {
    const airports = await Airport.find({});
    res.json(airports);
});

// GET /airports/nearby (Búsqueda geoespacial)
app.get('/airports/nearby', async (req, res) => {
    const { lng, lat, radius } = req.query;
    const codes = await redisGeo.georadius('airports-geo', lng, lat, radius, 'km');
    const airports = await Airport.find({ iata_faa: { $in: codes } });
    res.json(airports);
});

// GET /airports/popular (Ranking)
app.get('/airports/popular', async (req, res) => {
    const popularRaw = await redisPop.zrevrange('airport_popularity', 0, 9, 'WITHSCORES');
    
    // popularRaw = ["IATA1", "10", "IATA2", "5", ...]
    const result = [];
    for (let i = 0; i < popularRaw.length; i += 2) {
        const iata_code = popularRaw[i];
        const score = parseInt(popularRaw[i + 1]);
        const airportInfo = await Airport.findOne({ iata_faa: iata_code });
        
        if (airportInfo) {
            result.push({
                iata: iata_code,
                score: score,
                name: airportInfo.name,
                city: airportInfo.city
            });
        }
    }
    
    res.json(result);
});

// GET /airports/:iata_code (Detalle + Popularidad)
app.get('/airports/:iata_code', async (req, res) => {
    const { iata_code } = req.params;
    const airport = await Airport.findOne({ iata_faa: iata_code });

    if (!airport) {
        return res.status(404).json({ error: 'Aeropuerto no encontrado' });
    }

    // Incrementar popularidad
    await redisPop.zincrby('airport_popularity', 1, iata_code);
    await redisPop.expire('airport_popularity', 86400); // 24hs

    res.json(airport);
});

// POST /airports (Crear nuevo aeropuerto)
app.post('/airports', async (req, res) => {
    const newAirport = new Airport(req.body);
    await newAirport.save();
    
    // Si tiene coordenadas, agregarlo a Redis GEO
    if (newAirport.lat && newAirport.lng && newAirport.iata_faa) {
        await redisGeo.geoadd('airports-geo', newAirport.lng, newAirport.lat, newAirport.iata_faa);
    }
    
    res.status(201).json(newAirport);
});

// PUT /airports/:iata_code (Actualizar aeropuerto)
app.put('/airports/:iata_code', async (req, res) => {
    const { iata_code } = req.params;
    const updated = await Airport.findOneAndUpdate({ iata_faa: iata_code }, req.body, { new: true });
    
    if (!updated) return res.status(404).json({ error: 'Aeropuerto no encontrado' });

    // Actualizar Redis GEO (GEOADD sobrescribe/actualiza si ya existe)
    if (updated.lat && updated.lng) {
        await redisGeo.geoadd('airports-geo', updated.lng, updated.lat, updated.iata_faa);
    }

    res.json(updated);
});

// DELETE /airports/:iata_code (Eliminar aeropuerto)
app.delete('/airports/:iata_code', async (req, res) => {
    const { iata_code } = req.params;
    const deleted = await Airport.findOneAndDelete({ iata_faa: iata_code });
    
    if (!deleted) return res.status(404).json({ error: 'Aeropuerto no encontrado' });

    // Eliminar de Redis (Tanto del mapa geoespacial como del ranking)
    await redisGeo.zrem('airports-geo', iata_code);
    await redisPop.zrem('airport_popularity', iata_code);

    res.json({ message: 'Aeropuerto eliminado correctamente' });
});

app.listen(3000, () => console.log('Backend API en puerto 3000'));