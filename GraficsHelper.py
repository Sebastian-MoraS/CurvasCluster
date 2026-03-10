import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def generar_graficos_interactivos(df_pivot, n_clusters, temporada):
    """
    Genera gráficos con una curva promedio negra y gruesa (x5) destacada.
    """
    for i in range(n_clusters):
        # 1. Filtrar y ordenar datos del cluster
        df_cluster = df_pivot[df_pivot['cluster'] == i].copy()
        if df_cluster.empty: continue
        
        
        df_cluster = df_cluster.sort_values('hhmm')

        # 2. Calcular el promedio real del grupo
        df_promedio = df_cluster.groupby('hhmm')['P_kW_scaled'].mean().reset_index()

        # 3. Crear gráfica base (Curvas finas de medidores)
        fig = px.line(
            df_cluster, 
            x='hhmm', 
            y='P_kW_scaled', 
            color='meter_id', 
            title=f'Cluster {i+1} en {temporada}',
            labels={'hhmm': 'Hora del día', 'P_kW_scaled': 'Consumo (escalado)'}
        )
        
        
        fig.update_traces(line=dict(width=1), opacity=0.4)

        # 4. AGREGAR LA CURVA PROMEDIO (La "Master")
        # La ponemos al final para que sea la última traza en el índice
        fig.add_trace(
            go.Scatter(
                x=df_promedio['hhmm'],
                y=df_promedio['P_kW_scaled'],
                name='PROMEDIO TOTAL',
                line=dict(color='black', width=6), # Grosor x6 para asegurar visibilidad
                mode='lines'
            )
        )

        # 5. LÓGICA DE BOTONES (El "Cerebro" de la visibilidad)
        num_trazas_totales = len(fig.data)

        # Botón Mostrar Todos: Todas las curvas + Promedio = True
        botones = [
            dict(
                label="Mostrar todos",
                method="update",
                args=[{"visible": [True] * num_trazas_totales}, 
                      {"title": f"Cluster {i+1} ({temporada}): Perfiles vs Promedio"}]
            )
        ]

        # Botones individuales
        # MEJORA: Iteramos sobre las trazas reales de la figura para asegurar coincidencia
        # Excluimos la última traza porque sabemos que es el 'PROMEDIO TOTAL'
        trazas_medidores = fig.data[:-1] 
        
        for traza in trazas_medidores:
            m_id = traza.name # El nombre de la traza es el meter_id asignado por px.line
            # Visibilidad: False para casi todos, True para el medidor J y el Promedio
            visibilidad = []
            for t in fig.data:
                # Es visible si es la traza actual O si es el promedio (nombre fijo)
                es_visible = (t.name == m_id) or (t.name == 'PROMEDIO TOTAL')
                visibilidad.append(es_visible)
            
            botones.append(dict(
                label=f"Medidor {m_id}",
                method="update",
                args=[{"visible": visibilidad}, 
                      {"title": f"Cluster {i+1}: {m_id} comparado con Promedio"}]
            ))

        # 6. Configuración final del Layout
        fig.update_layout(
            updatemenus=[dict(
                buttons=botones,
                direction="down",
                showactive=True,
                x=1.15,
                y=1.1
            )],
            margin=dict(r=200),
            hovermode="closest" 
        )

        fig.show()