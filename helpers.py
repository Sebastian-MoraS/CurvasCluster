# helpers.py
import pandas as pd
import duckdb
from pathlib import Path
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans

def cargar_datos_a_duckdb(folder, con):
    """Detecta y carga archivos Parquet, Excel y CSV a una conexión DuckDB."""
    if not folder.exists():
        print(f" No se encuentra la carpeta {folder}")
        return

    for archivo in folder.glob("*"):
        # Limpieza del nombre para que sea válido en SQL
        nombre_tabla = archivo.stem.replace("-", "_").replace(" ", "_")
        
        if archivo.suffix == '.db': continue

        try:
            if archivo.suffix == '.parquet':
                # 1. Borramos cualquier rastro previo (sea tabla o vista)
                con.execute(f"DROP TABLE IF EXISTS {nombre_tabla} CASCADE")
                con.execute(f"DROP VIEW IF EXISTS {nombre_tabla} CASCADE")
                
                # 2. Creamos la vista limpia
                con.execute(f"CREATE OR REPLACE VIEW {nombre_tabla} AS SELECT * FROM '{archivo}'")
                print(f" Vista creada exitosamente: {nombre_tabla}")
            
            elif archivo.suffix in ['.xlsx', '.xls']:
                df_temp = pd.read_excel(archivo)
                con.register(nombre_tabla, df_temp)
                print(f" Tabla (Excel): {nombre_tabla}")
                
            elif archivo.suffix == '.csv' or archivo.name.startswith('032'):
                con.execute(f"CREATE OR REPLACE TABLE {nombre_tabla} AS SELECT * FROM read_csv_auto('{archivo}')")
                print(f" Tabla (CSV): {nombre_tabla}")
        
        except Exception as e:
            print(f" Error cargando {archivo.name}: {e}")

def encontrar_y_cargar(con, data_path):
    """Busca el primer archivo parquet y crea una vista genérica."""
    archivos = list(Path(data_path).glob("*.parquet"))
    
    if not archivos:
        raise FileNotFoundError(f"No se encontró ningún archivo .parquet en {data_path}")
    
    # Tomamos el primero que encuentre
    ruta_archivo = archivos[0]
    nombre_tabla = "raw_data_medidores" # Nombre genérico para el código posterior
    
    # Limpieza de seguridad para evitar errores de catálogo
    con.execute(f"DROP VIEW IF EXISTS {nombre_tabla} CASCADE")
    con.execute(f"DROP TABLE IF EXISTS {nombre_tabla} CASCADE")
    
    con.execute(f"CREATE VIEW {nombre_tabla} AS SELECT * FROM '{ruta_archivo}'")
    print(f"Archivo detectado y cargado como '{nombre_tabla}': {ruta_archivo.name}")
    return nombre_tabla


def ejecutar_pipeline_limpieza(con):
    """
    Saneamiento avanzado: 
    1. Calcula límites IQR basándose en los medidores ya filtrados en data_año.
    2. Imputa outliers usando el promedio histórico de la misma hora (hhmm).
    3. Elimina definitivamente medidores con potencia invertida.
    """
    query = """
    -- 1. Identificar medidores que sobrevivieron a 'procesar_medidores'
    -- (Esto asegura que no vuelvan a entrar los que tienen puro cero)
    CREATE OR REPLACE TEMP TABLE medidores_reales AS 
    SELECT DISTINCT mrid FROM data_año;

    -- 2. Calcular límites IQR por medidor y temporada
    CREATE OR REPLACE TEMP TABLE limites_iqr AS 
    SELECT 
        mrid, 
        tipo_curva,
        quantile_cont(P_kW, 0.25) AS Q1,
        quantile_cont(P_kW, 0.75) AS Q3,
        (Q3 - Q1) AS IQR_val,
        (Q1 - 1.5 * (Q3 - Q1)) AS lim_inf,
        (Q3 + 1.5 * (Q3 - Q1)) AS lim_sup
    FROM data_año
    GROUP BY 1, 2;

    -- 3. Crear tabla final con imputación de promedios horarios
    CREATE OR REPLACE TABLE data_final_cleaned AS 
    WITH raw_con_limites AS (
        SELECT 
            d.*, 
            l.lim_inf, 
            l.lim_sup,
            -- Extraemos hora y minuto para el promedio por bloque
            make_time(hour(d.timeStamp), minute(d.timeStamp), 0) AS hhmm
        FROM data_año d
        JOIN limites_iqr l ON d.mrid = l.mrid AND d.tipo_curva = l.tipo_curva
    ),
    promedios_sanos AS (
        -- Calculamos el promedio horario solo con datos que NO son outliers
        SELECT 
            mrid, 
            tipo_curva, 
            hhmm, 
            AVG(P_kW) as p_avg_sano
        FROM raw_con_limites
        WHERE P_kW BETWEEN lim_inf AND lim_sup
        GROUP BY 1, 2, 3
    )
    SELECT 
        r.mrid, 
        r.tipo_curva, 
        r.hhmm, 
        r.timeStamp, 
        r.Q_kVAr,
        -- Lógica de Imputación: Si es outlier, usa el promedio sano. Si no, dato real.
        CASE 
            WHEN r.P_kW < r.lim_inf OR r.P_kW > r.lim_sup THEN p.p_avg_sano
            ELSE r.P_kW 
        END AS P_kW
    FROM raw_con_limites r
    LEFT JOIN promedios_sanos p 
        ON r.mrid = p.mrid 
        AND r.tipo_curva = p.tipo_curva 
        AND r.hhmm = p.hhmm
    -- Último filtro de seguridad: Eliminar medidores invertidos (polaridad)
    WHERE r.mrid NOT IN (
        SELECT mrid FROM data_año GROUP BY 1 HAVING AVG(P_kW) < 0
    );
    """
    try:
        con.execute(query)
        print("Pipeline de limpieza completado: Tabla 'data_final_cleaned' lista.")
    except Exception as e:
        print(f"Error en el proceso de limpieza: {e}")

def procesar_medidores(con):
    """
    Crea la tabla 'data_año' aplicando filtros de integridad, 
    sincronización y actividad eléctrica real.
    """
    query = """
    -- 1. Crear referencia temporal (Reloj Maestro)
    CREATE OR REPLACE TEMP TABLE base_tiempo AS 
    SELECT ts_ref 
    FROM generate_series(TIMESTAMP '2024-01-01', TIMESTAMP '2025-01-01', INTERVAL 15 MINUTE) AS t(ts_ref);

    -- 2. Crear tabla principal con filtros técnicos estrictos
    CREATE OR REPLACE TABLE data_año AS 
    WITH join_datos AS (
        SELECT mi.*, ts.ts_ref 
        FROM raw_data_medidores mi
        LEFT JOIN base_tiempo ts ON mi.timeStamp = ts.ts_ref
    ),
    filtro_calidad AS (
        SELECT mrid
        FROM join_datos
        GROUP BY mrid
        HAVING 
            -- Integridad: Al menos 95% de presencia en el año
            (COUNT(*)::FLOAT * 100.0 / 35137) >= 95 
            
            -- Sincronización: Cero errores de alineación temporal
            AND COUNT(*) FILTER (WHERE ts_ref IS NULL) = 0
            
            -- Actividad Real: Al menos el 2% de los datos deben ser > 10W
            -- Esto descarta medidores que solo tienen 'ruido' de fondo
            AND (COUNT(*) FILTER (WHERE P_kW > 0.01)::FLOAT * 100.0 / COUNT(*)) > 2
            
            -- Vital para eliminar medidores en standby o desconectados
            AND MAX(P_kW) > 0.05
            
            -- Dinamismo: La curva no puede ser una línea perfectamente plana
            AND STDDEV(P_kW) > 0
            AND MAX(P_kW) >= 0.02
    )
    SELECT 
        j.*,
        -- Clasificación estacional para el análisis en Chile
        CASE 
            WHEN month(j.timeStamp) IN (1, 2, 3, 10, 11, 12) THEN 'Verano' 
            ELSE 'Invierno' 
        END AS tipo_curva
    FROM join_datos j
    INNER JOIN filtro_calidad f ON j.mrid = f.mrid;
    """
    try:
        con.execute(query)
        print(" Procesamiento de integridad y actividad completado: 'data_año' lista.")
    except Exception as e:
        print(f" Error en el procesamiento SQL: {e}")

def graficar_codo(X_scaled, titulo):
    wcss = []
    # Probamos de 1 a 10 (suele ser suficiente para perfiles de carga)
    for i in range(1, 11):
        # n_init='auto' evita advertencias en versiones nuevas de sklearn
        kmeans = KMeans(n_clusters=i, init="k-means++", n_init='auto', random_state=42)
        # IMPORTANTE: .T porque queremos agrupar medidores, no puntos horarios
        kmeans.fit(X_scaled.T) 
        wcss.append(kmeans.inertia_)

    plt.figure(figsize=(8, 4))
    plt.plot(range(1, 11), wcss, marker='o', color='#2ca02c') # Color verde ingeniería
    plt.title(f"Método del Codo - {titulo}")
    plt.xlabel("Número de Clusters (k)")
    plt.ylabel("Inercia (WCSS)")
    plt.grid(True, linestyle='--')
    plt.show()

def check_fantasma(con, id_fantasma='122120006684'):
    """
    Verifica si el medidor fantasma sigue presente en la tabla 'data_año'.
    Retorna True si encuentra el fantasma (Error), False si está limpio.
    """
    try:
        res = con.execute(f"SELECT COUNT(*) FROM data_año WHERE mrid = '{id_fantasma}'").fetchone()
        if res and res[0] > 0:
            print(f" ALERTA: El medidor fantasma {id_fantasma} fue detectado en 'data_año'.")
            return True
        print(f" OK: El medidor fantasma {id_fantasma} NO está en 'data_año'.")
        return False
    except Exception as e:
        print(f" No se pudo verificar el fantasma (¿Tabla data_año existe?): {e}")
        return False