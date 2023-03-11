const oracledb = require("oracledb");
const express = require("express");
const fs = require('fs');
const csv = require('csv-parser');
const app = express();
const { exec } = require('child_process');
const cssStyles = `
  body {
    background-color: #001029;
    text-align: center;
  }
  h1 {
    font-family: Arial, sans-serif;
    margin-top: 50px;
    margin-bottom: 50px;
    font-size: 32px;
    font-weight: bold;
    color: white;
  }
  table {
    text-align: center;
    border-collapse: collapse;
    margin: 20px auto;
    font-family: Arial, sans-serif;
    font-size: 14px;
    color: #ffffff;
  }
  th, td {
    text-align: center;
    border: 1px solid #001029;
    padding: 8px;
    text-align: left;
  }
  th {
    text-align: center;
    background-color: #c76700;
    color: white;
  }
`;
const { Parser } = require('csv-parse');
const { pipeline } = require('stream');
const util = require('util');
const pipelineAsync = util.promisify(pipeline);

app.get("/cargarTemporal", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
      autoCommit: true // habilitar autocommit
    });

    await conn.execute(`
    CREATE TABLE registros(
      nombre_victima VARCHAR2(50),
      apellido_victima VARCHAR2(50),
      direccion_victima VARCHAR2(100),
      fecha_primera_sospecha DATE,
      fecha_confirmacion DATE,
      fecha_muerte DATE,
      estado_victima VARCHAR2(50),
      nombre_asociado VARCHAR2(50),
      apellido_asociado VARCHAR2(50),
      fecha_conocio DATE,
      contacto_fisico VARCHAR2(50),
      fecha_inicio_contacto DATE,
      fecha_fin_contacto DATE,
      nombre_hospital VARCHAR2(100),
      direccion_hospital VARCHAR2(100),
      ubicacion_victima VARCHAR2(100),
      fecha_llegada DATE,
      fecha_retiro DATE,
      tratamiento VARCHAR2(50),
      efectividad NUMBER(2),
      fecha_inicio_tratamiento DATE,
      fecha_fin_tratamiento DATE,
      efectividad_en_victima NUMBER(2)
    )`);

    await conn.execute(`ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'`);

await conn.execute(`
ALTER TABLE registros MODIFY 
  (fecha_primera_sospecha DATE,
   fecha_confirmacion DATE,
   fecha_muerte DATE,
   fecha_conocio DATE,
   fecha_inicio_contacto DATE,
   fecha_fin_contacto DATE,
   fecha_llegada DATE,
   fecha_retiro DATE,
   fecha_inicio_tratamiento DATE,
   fecha_fin_tratamiento DATE)`);

   // Leer archivo CSV
    const results = [];
    const stream = fs.createReadStream('./database.csv').pipe(csv());

    stream.on('data', (data) => results.push(data));
    await new Promise((resolve, reject) => {
      stream.on('end', () => resolve());
      stream.on('error', (err) => reject(err));
    });

    const insertPromises = results.map(async (row) => {
      const keys = Object.keys(row);
      const values = Object.values(row).map(value => {
        return value === '' ? null : typeof value === 'number' ? value.toString() : value;
      });

      // Convertir las fechas al formato adecuado
      const FECHA_PRIMERA_SOSPECHA = formatDate(row.FECHA_PRIMERA_SOSPECHA);
      const FECHA_CONFIRMACION = formatDate(row.FECHA_CONFIRMACION);
      const FECHA_MUERTE = formatDate(row.FECHA_MUERTE);
      const FECHA_CONOCIO = formatDate(row.FECHA_CONOCIO);
      const FECHA_INICIO_CONTACTO = formatDate(row.FECHA_INICIO_CONTACTO);
      const FECHA_FIN_CONTACTO = formatDate(row.FECHA_FIN_CONTACTO);
      const FECHA_LLEGADA = formatDate(row.FECHA_LLEGADA);
      const FECHA_RETIRO = formatDate(row.FECHA_RETIRO);
      const FECHA_INICIO_TRATAMIENTO = formatDate(row.FECHA_INICIO_TRATAMIENTO);
      const FECHA_FIN_TRATAMIENTO = formatDate(row.FECHA_FIN_TRATAMIENTO);

      // Crear objeto con las fechas convertidas
      const convertedRow = {
        ...row,
        FECHA_PRIMERA_SOSPECHA,
        FECHA_CONFIRMACION,
        FECHA_MUERTE,
        FECHA_CONOCIO,
        FECHA_INICIO_CONTACTO,
        FECHA_FIN_CONTACTO,
        FECHA_LLEGADA,
        FECHA_RETIRO,
        FECHA_INICIO_TRATAMIENTO,
        FECHA_FIN_TRATAMIENTO,
      };

      const placeholders = keys.map(key => ':' + key).join(',');
      const sql = `INSERT INTO registros (${keys.join(', ')}) VALUES (${placeholders})`;
      await conn.execute(sql, convertedRow);
    });

    await Promise.all(insertPromises);

    console.log('Datos cargados correctamente');

    // Función para convertir fecha a formato adecuado
    function formatDate(dateString) {
      if (!dateString) return null;
      const [day, month, year] = dateString.split('/');
      const [year2,time] = year.split(' ');
      const date = new Date(`${year2}-${month}-${day}T00:00:00Z`);
      if (isNaN(date.getTime())) {
        return null;
      }
      const formattedDate = date.toISOString().slice(0, 19).replace('T', ' ');
      console.log(formattedDate,date);
      return formattedDate;
    }
    
    await conn.commit();
    res.status(200).send("Información: La temporal se ha creado y llenado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error obteniendo registros");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/cargarModelo", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });

     //Tabla asociado
     await conn.execute(`
     CREATE TABLE asociado(
       id_asociado NUMBER NOT NULL,
       nombre VARCHAR2(50) NOT NULL,
       apellidos VARCHAR2(50) NOT NULL,
       CONSTRAINT pk_asociado PRIMARY KEY (id_asociado)
     )
   `);
    await conn.execute(`
      CREATE SEQUENCE seq_asociado
    `);
    await conn.execute(`
      CREATE TRIGGER trg_asociado
      BEFORE INSERT ON asociado
      FOR EACH ROW
      BEGIN
        SELECT seq_asociado.nextval INTO :new.id_asociado FROM dual;
      END;
    `);
    await conn.execute(`
      INSERT INTO asociado(id_asociado, nombre, apellidos)
      SELECT COALESCE((SELECT MAX(id_asociado) FROM asociado), 0) + 1, nombre_asociado, apellido_asociado
      FROM (
        SELECT DISTINCT nombre_asociado, apellido_asociado
        FROM registros
        WHERE nombre_asociado IS NOT NULL AND apellido_asociado IS NOT NULL
      ) r
      WHERE NOT EXISTS (
        SELECT 1 FROM asociado
        WHERE nombre = r.nombre_asociado AND apellidos = r.apellido_asociado
      )
    `);
    await conn.commit();
    console.log("Tabla asociado creada y llenada");
    //---------------------------------------------------------------------------
    //Tabla victima
    await conn.execute(`
      CREATE TABLE victima(
        id_victima NUMBER NOT NULL,
        nombre VARCHAR2(50) NOT NULL,
        apellidos VARCHAR2(50) NOT NULL,
        direccion VARCHAR2(150) NOT NULL,
        estado VARCHAR2(100) NOT NULL,
        fecha_sospecha DATE NOT NULL,
        fecha_confirmacion DATE NOT NULL,
        fecha_muerte DATE,
        CONSTRAINT pk_victima PRIMARY KEY (id_victima)
        )
    `);
    await conn.execute(`
      CREATE SEQUENCE seq_victima
    `);
    await conn.execute(`
      CREATE TRIGGER trg_victima
      BEFORE INSERT ON victima
      FOR EACH ROW
      BEGIN
        SELECT seq_victima.nextval INTO :new.id_victima FROM dual;
      END;
    `);
    await conn.execute(`
      INSERT INTO victima(id_victima, nombre, apellidos, direccion, estado, fecha_sospecha, fecha_confirmacion, fecha_muerte)
      SELECT 
        COALESCE((SELECT MAX(id_victima) FROM victima), 0) + 1, nombre_victima, apellido_victima, direccion_victima, estado_victima, fecha_primera_sospecha, fecha_confirmacion, fecha_muerte
      FROM (
        SELECT DISTINCT 
          nombre_victima, apellido_victima, direccion_victima, estado_victima, fecha_primera_sospecha, fecha_confirmacion, fecha_muerte
        FROM registros
        WHERE 
          nombre_victima IS NOT NULL 
          AND apellido_victima IS NOT NULL 
          AND direccion_victima IS NOT NULL 
          AND estado_victima IS NOT NULL 
          AND fecha_primera_sospecha IS NOT NULL 
          AND fecha_confirmacion IS NOT NULL 
          AND (fecha_muerte IS NULL OR fecha_muerte IS NOT NULL)
      ) r
      WHERE NOT EXISTS (
        SELECT 1 FROM victima WHERE nombre = r.nombre_victima AND apellidos = r.apellido_victima AND direccion = r.direccion_victima
      )
    `);
    await conn.commit();
    console.log("Tabla victima creada y llenada");
    //---------------------------------------------------------------------------
    //Tabla hospital
    await conn.execute(`
      CREATE TABLE hospital(
        id_hospital NUMBER NOT NULL,
        nombre VARCHAR2(50) NOT NULL,
        direccion VARCHAR2(150) NOT NULL,
        CONSTRAINT pk_hospital PRIMARY KEY (id_hospital)
        )
    `);
    await conn.execute(`
      CREATE SEQUENCE seq_hospital
    `);
    await conn.execute(`
      CREATE TRIGGER trg_hospital
      BEFORE INSERT ON hospital
      FOR EACH ROW
      BEGIN
        SELECT seq_hospital.nextval INTO :new.id_hospital FROM dual;
      END;
    `);
    await conn.execute(`
      INSERT INTO hospital(id_hospital, nombre, direccion)
      SELECT COALESCE((SELECT MAX(id_hospital) FROM hospital), 0) + 1, nombre_hospital, direccion_hospital
      FROM (
        SELECT DISTINCT nombre_hospital, direccion_hospital
        FROM registros
        WHERE nombre_hospital IS NOT NULL AND direccion_hospital IS NOT NULL
      ) r
      WHERE NOT EXISTS (
        SELECT 1 FROM hospital
        WHERE nombre = r.nombre_hospital
      )
    `);
    await conn.commit();
    console.log("Tabla hospital creada y llenada");
    //---------------------------------------------------------------------------
    //Tabla tratamiento
    await conn.execute(`
      CREATE TABLE tratamiento(
        id_tratamiento NUMBER NOT NULL,
        descripcion VARCHAR2(150) NOT NULL,
        efectividad NUMBER NOT NULL,
        CONSTRAINT pk_tratamiento PRIMARY KEY (id_tratamiento)
        )
    `);
    await conn.execute(`
      CREATE SEQUENCE seq_tratamiento
    `);
    await conn.execute(`
      CREATE TRIGGER trg_tratamiento
      BEFORE INSERT ON tratamiento
      FOR EACH ROW
      BEGIN
        SELECT seq_tratamiento.nextval INTO :new.id_tratamiento FROM dual;
      END;
    `);
    await conn.execute(`
      INSERT INTO tratamiento(id_tratamiento, descripcion, efectividad)
      SELECT COALESCE((SELECT MAX(id_tratamiento) FROM tratamiento), 0) + 1, tratamiento, efectividad
      FROM (
        SELECT DISTINCT tratamiento, efectividad
        FROM registros
        WHERE tratamiento IS NOT NULL AND efectividad IS NOT NULL
      ) r
      WHERE NOT EXISTS (
        SELECT 1 FROM tratamiento
        WHERE descripcion = r.tratamiento AND efectividad = r.efectividad
      )
    `);
    await conn.commit();
    console.log("Tabla tratamiento creada y llenada");
    //---------------------------------------------------------------------------
    //Tabla detalle_hospital
    await conn.execute(`
      CREATE TABLE detalle_hospital(
        id_detalle NUMBER NOT NULL,
        id_victima NUMBER NOT NULL,
        id_hospital NUMBER NOT NULL,
        CONSTRAINT fk_detalle_hospital_v FOREIGN KEY (id_victima) REFERENCES victima(id_victima),
        CONSTRAINT fk_detalle_hospital_h FOREIGN KEY (id_hospital) REFERENCES hospital(id_hospital)
        )
    `);
    await conn.execute(`
      CREATE SEQUENCE seq_detalle_hospital
    `);
    await conn.execute(`
      CREATE TRIGGER trg_detalle_hospital
      BEFORE INSERT ON detalle_hospital
      FOR EACH ROW
      BEGIN
        SELECT seq_detalle_hospital.nextval INTO :new.id_detalle FROM dual;
      END;
    `);
    await conn.execute(`
      INSERT INTO detalle_hospital (id_detalle, id_hospital, id_victima)
      SELECT 
        COALESCE(MAX(dh.id_detalle), 0) + ROW_NUMBER() OVER (ORDER BY d.id_hospital, v.id_victima),
        d.id_hospital, 
        v.id_victima
      FROM 
        (SELECT r.*, h.id_hospital
        FROM registros r
        JOIN hospital h ON r.nombre_hospital = h.nombre AND r.direccion_hospital = h.direccion) d
      JOIN 
        victima v ON d.nombre_victima = v.nombre AND d.apellido_victima = v.apellidos AND d.direccion_victima = v.direccion
      LEFT JOIN 
        detalle_hospital dh ON dh.id_hospital = d.id_hospital AND dh.id_victima = v.id_victima
      WHERE dh.id_hospital IS NULL AND dh.id_victima IS NULL
      GROUP BY d.id_hospital, v.id_victima
    `);
    await conn.commit();
    console.log("Tabla detalle_hospital creada y llenada");
    //---------------------------------------------------------------------------
    //Tabla ubicacion
    await conn.execute(`
      CREATE TABLE ubicacion(
        id_ubicacion NUMBER NOT NULL,
        direccion VARCHAR2(150) NOT NULL,
        fecha_llegada DATE NOT NULL,
        fecha_retiro DATE NOT NULL,
        id_victima NUMBER NOT NULL,
        CONSTRAINT fk_ubicacion FOREIGN KEY (id_victima) REFERENCES victima(id_victima)
        )
    `);
    await conn.execute(`
      CREATE SEQUENCE seq_ubicacion
    `);
    await conn.execute(`
      CREATE TRIGGER trg_ubicacion
      BEFORE INSERT ON ubicacion
      FOR EACH ROW
      BEGIN
        SELECT seq_ubicacion.nextval INTO :new.id_ubicacion FROM dual;
      END;
    `);
    await conn.execute(`
      INSERT INTO ubicacion (id_ubicacion, direccion, fecha_llegada, fecha_retiro, id_victima)
      SELECT DISTINCT 
        COALESCE((SELECT MAX(id_ubicacion) FROM ubicacion), 0) + 1,
        r.ubicacion_victima, 
        r.fecha_llegada, 
        r.fecha_retiro, 
        v.id_victima
      FROM 
        registros r
        JOIN victima v ON r.nombre_victima = v.nombre AND r.apellido_victima = v.apellidos AND r.direccion_victima = v.direccion
      WHERE r.ubicacion_victima IS NOT NULL AND r.fecha_llegada IS NOT NULL AND r.fecha_retiro IS NOT NULL AND v.id_victima IS NOT NULL 
        AND NOT EXISTS (
          SELECT 1 
          FROM ubicacion u 
          WHERE u.direccion = r.ubicacion_victima AND u.fecha_llegada = r.fecha_llegada AND u.fecha_retiro = r.fecha_retiro AND u.id_victima = v.id_victima
        )
    `);
    await conn.commit();
    console.log("Tabla ubicacion creada y llenada");
    //---------------------------------------------------------------------------
    //Tabla victima_efectividad
    await conn.execute(`
      CREATE TABLE victima_efectividad(
        id_victima_efectividad NUMBER NOT NULL,
        efectividad_victima NUMBER NOT NULL,
        fecha_inicio DATE NOT NULL,
        fecha_fin DATE NOT NULL,
        id_tratamiento NUMBER NOT NULL,
        id_victima NUMBER NOT NULL,
        CONSTRAINT fk_victima_efectividad_t FOREIGN KEY (id_tratamiento) REFERENCES tratamiento(id_tratamiento),
        CONSTRAINT fk_victima_efectividad_v FOREIGN KEY (id_victima) REFERENCES victima(id_victima)
        )
    `);
    await conn.execute(`
      CREATE SEQUENCE seq_victima_efectividad
    `);
    await conn.execute(`
      CREATE TRIGGER trg_victima_efectividad
      BEFORE INSERT ON victima_efectividad
      FOR EACH ROW
      BEGIN
        SELECT seq_victima_efectividad.nextval INTO :new.id_victima_efectividad FROM dual;
      END;
    `);
    await conn.execute(`
    INSERT INTO victima_efectividad (id_victima_efectividad, id_victima, id_tratamiento, efectividad_victima, fecha_inicio, fecha_fin)
    SELECT COALESCE(MAX(ve.id_victima_efectividad), 0) + 1, v.id_victima, t.id_tratamiento, r.efectividad_en_victima, r.fecha_inicio_tratamiento, r.fecha_fin_tratamiento
    FROM victima v
    JOIN registros r ON v.nombre = r.nombre_victima AND v.apellidos = r.apellido_victima AND v.direccion = r.direccion_victima
    JOIN tratamiento t ON t.descripcion = r.tratamiento
    LEFT JOIN victima_efectividad ve ON ve.id_victima = v.id_victima AND ve.id_tratamiento = t.id_tratamiento
    WHERE ve.id_victima_efectividad IS NULL
    GROUP BY v.id_victima, t.id_tratamiento, r.efectividad_en_victima, r.fecha_inicio_tratamiento, r.fecha_fin_tratamiento
    `);
    await conn.commit();
    console.log("Tabla victima_efectividad creada y llenada");
    //---------------------------------------------------------------------------
    //Tabla contacto
    await conn.execute(`
      CREATE TABLE contacto(
        id_contacto NUMBER NOT NULL,
        tipo VARCHAR2(50) NOT NULL,
        fecha_inicio DATE NOT NULL,
        fecha_fin DATE NOT NULL,
        id_asociado NUMBER NOT NULL,
        id_victima NUMBER NOT NULL,
        CONSTRAINT fk_contacto_a FOREIGN KEY (id_asociado) REFERENCES asociado(id_asociado),
        CONSTRAINT fk_contacto_v FOREIGN KEY (id_victima) REFERENCES victima(id_victima)
        )
    `);
    await conn.execute(`
      CREATE SEQUENCE seq_contacto
    `);
    await conn.execute(`
      CREATE TRIGGER trg_contacto
      BEFORE INSERT ON contacto
      FOR EACH ROW
      BEGIN
        SELECT seq_contacto.nextval INTO :new.id_contacto FROM dual;
      END;
    `);
    await conn.execute(`
    INSERT INTO contacto (id_contacto, tipo, fecha_inicio, fecha_fin, id_victima, id_asociado)
    SELECT COALESCE(MAX(id_contacto), 0) + 1, r.contacto_fisico, r.fecha_inicio_contacto, r.fecha_fin_contacto, v.id_victima, a.id_asociado
    FROM registros r
    JOIN victima v ON v.nombre = r.nombre_victima AND v.apellidos = r.apellido_victima
    JOIN asociado a ON a.nombre = r.nombre_asociado AND a.apellidos = r.apellido_asociado
    LEFT JOIN contacto c ON c.tipo = r.contacto_fisico AND c.fecha_inicio = r.fecha_inicio_contacto AND c.fecha_fin = r.fecha_fin_contacto AND c.id_victima = v.id_victima AND c.id_asociado = a.id_asociado
    WHERE c.id_contacto IS NULL
    AND r.contacto_fisico IS NOT NULL
    AND r.fecha_inicio_contacto IS NOT NULL
    AND r.fecha_fin_contacto IS NOT NULL
    AND v.id_victima IS NOT NULL
    AND a.id_asociado IS NOT NULL
    GROUP BY r.contacto_fisico, r.fecha_inicio_contacto, r.fecha_fin_contacto, v.id_victima, a.id_asociado
    `);
    await conn.commit();
    console.log("Tabla contacto creada y llenada");
    //---------------------------------------------------------------------------
    //Tabla detalle_conocido
    await conn.execute(`
      CREATE TABLE detalle_conocido(
        id_detalle_conocido NUMBER NOT NULL,
        fecha_conocido DATE NOT NULL,
        id_asociado NUMBER NOT NULL,
        id_victima NUMBER NOT NULL,
        CONSTRAINT fk_detalle_conocido_a FOREIGN KEY (id_asociado) REFERENCES asociado(id_asociado),
        CONSTRAINT fk_detalle_conocido_v FOREIGN KEY (id_victima) REFERENCES victima(id_victima)
        )
    `);
    await conn.execute(`
      CREATE SEQUENCE seq_detalle_conocido
    `);
    await conn.execute(`
      CREATE TRIGGER trg_detalle_conocido
      BEFORE INSERT ON detalle_conocido
      FOR EACH ROW
      BEGIN
        SELECT seq_detalle_conocido.nextval INTO :new.id_detalle_conocido FROM dual;
      END;
    `);
    await conn.execute(`
      INSERT INTO detalle_conocido (id_detalle_conocido, fecha_conocido, id_asociado, id_victima)
      SELECT 
        COALESCE(MAX(dh.id_detalle_conocido), 0) + ROW_NUMBER() OVER (ORDER BY d.id_asociado, v.id_victima),
        MAX(r.fecha_conocio),
        d.id_asociado,
        v.id_victima
      FROM 
        (SELECT r.*, h.id_asociado
        FROM registros r
        JOIN asociado h ON r.nombre_asociado = h.nombre AND r.apellido_asociado = h.apellidos) d
      JOIN 
        victima v ON d.nombre_victima = v.nombre AND d.apellido_victima = v.apellidos AND d.direccion_victima = v.direccion
      LEFT JOIN 
        detalle_conocido dh ON dh.id_asociado = d.id_asociado AND dh.id_victima = v.id_victima
      LEFT JOIN 
        registros r ON r.nombre_victima = v.nombre AND r.apellido_victima = v.apellidos AND r.direccion_victima = v.direccion
      WHERE dh.id_asociado IS NULL AND dh.id_victima IS NULL
      GROUP BY d.id_asociado, v.id_victima
    `);
    await conn.commit();
    console.log("Tabla detalle_conocido creada y llenada");
    //---------------------------------------------------------------------------
    res.status(200).send("Información: El modelo se ha creado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido crear el modelo");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/eliminarTemporal", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    await conn.execute(`
      DROP TABLE REGISTROS
    `);
    console.log("Información: La tabla temporal ha sido eliminada con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido eliminar la tanla temporal");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/eliminarModelo", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    await conn.execute(`
      BEGIN
        FOR cur_rec IN (SELECT trigger_name FROM user_triggers)
        LOOP
          EXECUTE IMMEDIATE ('DROP TRIGGER ' || cur_rec.trigger_name);
        END LOOP;
      END;
    `);
    await conn.execute(`
      BEGIN
        FOR cur_rec IN (SELECT sequence_name FROM user_sequences)
        LOOP
          EXECUTE IMMEDIATE ('DROP SEQUENCE ' || cur_rec.sequence_name);
        END LOOP;
      END;
    `);
    await conn.execute(`
      DROP TABLE CONTACTO
    `);
    await conn.execute(`
      DROP TABLE DETALLE_CONOCIDO
    `);
    await conn.execute(`
      DROP TABLE VICTIMA_EFECTIVIDAD
    `);
    await conn.execute(`
      DROP TABLE DETALLE_HOSPITAL
    `);
    await conn.execute(`
      DROP TABLE UBICACION
    `);
    await conn.execute(`
      DROP TABLE HOSPITAL
    `);
    await conn.execute(`
      DROP TABLE TRATAMIENTO
    `);
    await conn.execute(`
      DROP TABLE ASOCIADO
    `);
    await conn.execute(`
      DROP TABLE VICTIMA
    `);
    await conn.commit();
    console.log("Información: El modelo ha sido eliminado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido eliminar el modelo");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta1", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
      SELECT h.nombre, h.direccion, COUNT(CASE WHEN v.estado = 'Muerte' OR v.fecha_muerte IS NOT NULL THEN 1 ELSE NULL END) AS victimas_fallecidas
      FROM detalle_hospital dh
      INNER JOIN hospital h ON dh.id_hospital = h.id_hospital
      INNER JOIN victima v ON dh.id_victima = v.id_victima
      GROUP BY h.id_hospital, h.nombre, h.direccion
    `);
    let htmlTable = "<table><tr><th>Nombre</th><th>Dirección</th><th>Víctimas fallecidas</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td><td>${row[2]}</td></tr>`;
    }

    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 1</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>HOSPITALES CON VICTIMAS FALLECIDAS</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 1 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 1");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta2", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
      SELECT DISTINCT v.nombre, v.apellidos
      FROM victima v
      INNER JOIN victima_efectividad ve ON v.id_victima = ve.id_victima
      INNER JOIN tratamiento t ON ve.id_tratamiento = t.id_tratamiento
      WHERE t.descripcion = 'Transfusiones de sangre' AND ve.efectividad_victima > 5
    `);
    let htmlTable = "<table><tr><th>Nombre</th><th>Apellidos</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td></tr>`;
    }
    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 2</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>VICTIMAS DE TRANSFUCIÓN DE SANGRE CON EFECTIVIDAD > 5</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 2 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 2");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta3", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
      SELECT DISTINCT v.nombre, v.apellidos, v.direccion
      FROM victima v
      INNER JOIN detalle_conocido dc ON v.id_victima = dc.id_victima
      WHERE v.estado = 'Muerte' OR v.fecha_muerte IS NOT NULL
      GROUP BY v.id_victima, v.nombre, v.apellidos, v.direccion
      HAVING COUNT(dc.id_asociado) > 3
    `);
    let htmlTable = "<table><tr><th>Nombre</th><th>Apellidos</th><th>Direccion</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td><td>${row[2]}</td></tr>`;
    }
    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 3</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>VICTIMAS FALLECIDAS CON MÁS DE 3 ASOCIADOS</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 3 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 3");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta4", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
      SELECT DISTINCT v.nombre, v.apellidos
      FROM contacto c
      INNER JOIN asociado a ON c.id_asociado = a.id_asociado
      INNER JOIN victima v ON c.id_victima = v.id_victima
      WHERE v.estado = 'Suspendida'
      AND c.tipo = 'Beso'
      GROUP BY v.id_victima, v.nombre, v.apellidos
      HAVING COUNT(DISTINCT a.id_asociado) > 2
    `);
    let htmlTable = "<table><tr><th>Nombre</th><th>Apellidos</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td></tr>`;
    }
    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 4</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>VICTIMAS SUSPENDIDAS CON UN CONTACTO TIPO BESO > 2 DE SUS ASOCIADOS</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 4 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 4");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta5", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
      SELECT * FROM (
        SELECT v.nombre, v.apellidos, v.direccion, COUNT(*) AS tratamientos_de_oxigeno
        FROM victima v
        INNER JOIN victima_efectividad ve ON v.id_victima = ve.id_victima
        INNER JOIN tratamiento t ON ve.id_tratamiento = t.id_tratamiento AND t.descripcion = 'Oxigeno'
        GROUP BY v.id_victima, v.nombre, v.apellidos, v.direccion
        ORDER BY COUNT(*) DESC
      ) WHERE ROWNUM <= 5
    `);
    let htmlTable = "<table><tr><th>Nombre</th><th>Apellidos</th><th>Dirección</th><th>Cantidad</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td><td>${row[2]}</td><td>${row[3]}</td></tr>`;
    }
    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 5</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>TOP 5 VICTIMAS CON MÁS TRATAMIENTOS DE OXIGENO</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 5 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 5");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta6", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
      SELECT DISTINCT v.nombre, v.apellidos, v.fecha_muerte
      FROM victima v
      INNER JOIN ubicacion u ON v.id_victima = u.id_victima
      INNER JOIN victima_efectividad ve ON v.id_victima = ve.id_victima
      INNER JOIN tratamiento t ON ve.id_tratamiento = t.id_tratamiento
      WHERE u.direccion = '1987 Delphine Well'
      AND t.descripcion = 'Manejo de la presion arterial'
    `);
    let htmlTable = "<table><tr><th>Nombre</th><th>Apellidos</th><th>Fecha de fallecimiento</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td><td>${row[2]}</td></tr>`;
    }
    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 6</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>VICTIMAS EN DELPHINE WELL CON PRESIÓN ARTERIAL</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 6 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 6");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta7", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
    SELECT v.nombre, v.apellidos, v.direccion AS cantidad_asociados
    FROM victima v
    LEFT JOIN detalle_conocido dc ON v.id_victima = dc.id_victima
    WHERE v.id_victima IN (
        SELECT ve.id_victima
        FROM victima_efectividad ve
        GROUP BY ve.id_victima
        HAVING COUNT(ve.id_tratamiento) = 2
    )
    GROUP BY v.id_victima, v.nombre, v.apellidos, v.direccion
    HAVING COUNT(dc.id_asociado) <= 2
    `);
    //Consulta que muestra las victimas con 2 tratamientos
    /*const result = await conn.execute(`
    SELECT v.nombre, v.apellidos, v.direccion
    FROM victima v
    WHERE v.id_victima IN (
        SELECT ve.id_victima
        FROM victima_efectividad ve
        GROUP BY ve.id_victima
        HAVING COUNT(ve.id_tratamiento) = 2
    )
    `);*/
    let htmlTable = "<table><th>Nombre</th><th>Apellidos</th><th>Direccion</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td><td>${row[2]}</td></tr>`;
    }
    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 7</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>VICTIMAS CON MENOS DE 2 ASOCIADOS Y CON 2 TRATAMIENTOS</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 7 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 7");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta8", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
      SELECT * FROM (
        SELECT EXTRACT(MONTH FROM v.fecha_sospecha) AS mes, v.nombre, v.apellidos, COUNT(*) AS num_tratamientos 
        FROM victima v 
        JOIN victima_efectividad ve ON v.id_victima = ve.id_victima 
        JOIN tratamiento t ON t.id_tratamiento = ve.id_tratamiento 
        GROUP BY EXTRACT(MONTH FROM v.fecha_sospecha), v.nombre, v.apellidos 
        ORDER BY num_tratamientos DESC
      ) WHERE ROWNUM <= 10
      UNION ALL
      SELECT * FROM (
        SELECT EXTRACT(MONTH FROM v.fecha_sospecha) AS mes, v.nombre, v.apellidos, COUNT(*) AS num_tratamientos 
        FROM victima v 
        JOIN victima_efectividad ve ON v.id_victima = ve.id_victima 
        JOIN tratamiento t ON t.id_tratamiento = ve.id_tratamiento 
        GROUP BY EXTRACT(MONTH FROM v.fecha_sospecha), v.nombre, v.apellidos 
        ORDER BY num_tratamientos ASC
      ) WHERE ROWNUM <= 10
    `);
    let htmlTable = "<table><tr><th>No.Mes</th><th>Nombre</th><th>Apellidos</th><th>Cantidad de tratamientos</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td><td>${row[2]}</td><td>${row[3]}</td></tr>`;
    }
    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 8</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>TOP DE VICTIMAS CON MÁS Y MENOS TRATAMIENTOS</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 8 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 8");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta9", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
    SELECT h.nombre, h.direccion, COUNT(dh.id_victima) AS num_victimas, 
      ROUND(COUNT(dh.id_victima) / CAST((SELECT COUNT(*) FROM detalle_hospital) AS FLOAT) * 100, 2) AS porcentaje_victimas
    FROM hospital h
    JOIN detalle_hospital dh ON h.id_hospital = dh.id_hospital
    GROUP BY h.id_hospital, h.nombre, h.direccion
    `);
    let htmlTable = "<table><tr><th>Nombre</th><th>Dirección</th><th>Cantidad</th><th>Porcentaje (%)</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td><td>${row[2]}</td><td>${row[3]}</td></tr>`;
    }
    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 9</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>PORCENTAJE DE VICTIMAS DE CADA HOSPITAL</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 9 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 9");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.get("/consulta10", async (req, res) => {
  let conn;
  try {
    // Conectarse a la base de datos en el contenedor Docker
    conn = await oracledb.getConnection({
      user: "alexis",
      password: "pass123",
      connectString: "localhost:1521/XE",
    });
    const result = await conn.execute(`
    SELECT nombre, direccion, tipo_contacto, ROUND(porcentaje, 2) AS porcentaje
    FROM (
      SELECT h.nombre, h.direccion, c.tipo AS tipo_contacto, COUNT(*) * 100.0 / (
        SELECT COUNT(*)
        FROM detalle_hospital dh2
        INNER JOIN hospital h2 ON h2.id_hospital = dh2.id_hospital
        WHERE h2.id_hospital = h.id_hospital
      ) AS porcentaje,
      ROW_NUMBER() OVER (PARTITION BY h.id_hospital ORDER BY COUNT(*) DESC) AS rn
      FROM hospital h
      INNER JOIN detalle_hospital dh ON h.id_hospital = dh.id_hospital
      INNER JOIN (
          SELECT id_victima, MAX(tipo) AS tipo
          FROM contacto
          GROUP BY id_victima
      ) c ON c.id_victima = dh.id_victima
      WHERE dh.id_hospital = (SELECT MAX(id_hospital) FROM detalle_hospital dh2 WHERE dh2.id_victima = dh.id_victima)
      GROUP BY h.id_hospital, h.nombre, h.direccion, c.tipo
    )
    WHERE rn = 1
    `);
    let htmlTable = "<table><tr><th>Nombre</th><th>Direccion</th><th>Tipo</th><th>Porcentaje (%)</th></tr>";
    for (let row of result.rows) {
      htmlTable += `<tr><td>${row[0]}</td><td>${row[1]}</td><td>${row[2]}</td><td>${row[3]}</td></tr>`;
    }
    // Construir la respuesta
    const htmlResponse = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Consulta 10</title>
        <style>${cssStyles}</style>
      </head>
      <body>
        <h1>CONTACTO FISICO MÁS COMÚN POR HOSPITAL</h1>
        ${htmlTable}
      </body>
    </html>
    `;
    // Enviar la respuesta
    res.send(htmlResponse);
    console.log("Información: La consulta 10 se ha realizado con éxito");
  } catch (err) {
    console.error(err);
    res.status(500).send("Error: No se ha podido realizar la consulta 10");
  } finally {
    if (conn) {
      // Cerrar la conexión
      await conn.close();
    }
  }
});

app.listen(3000, () => {
  console.log("Servidor corriendo en el puerto 3000");
});