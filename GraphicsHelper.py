import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import matplotlib.pyplot as plt

def generar_graficos_interactivos(df_pivot, n_clusters, temporada):
    """
    Genera gráficos con una curva promedio negra y gruesa (x5) destacada.
    """
    for i in range(n_clusters):

        df_cluster = df_pivot[df_pivot['cluster'] == i].copy()
        if df_cluster.empty: continue
        
        df_cluster = df_cluster.sort_values('hhmm')
        df_promedio = df_cluster.groupby('hhmm')['P_kW_scaled'].mean().reset_index()

        fig = px.line(
            df_cluster, 
            x='hhmm', 
            y='P_kW_scaled', 
            color='meter_id', 
            title=f'Cluster {i+1} en {temporada}',
            labels={'hhmm': 'Hora del día', 'P_kW_scaled': 'Consumo (escalado)'}
        )
                
        fig.update_traces(line=dict(width=1), opacity=0.4)

        fig.add_trace(
            go.Scatter(
                x=df_promedio['hhmm'],
                y=df_promedio['P_kW_scaled'],
                name='PROMEDIO TOTAL',
                line=dict(color='black', width=6), # Grosor x6 para asegurar visibilidad
                mode='lines'
            )
        )
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


def generar_graficos_estaticos(df_pivot, n_clusters, temporada):

    for i in range(n_clusters):

        df_cluster = df_pivot[df_pivot['cluster'] == i].copy()
        if df_cluster.empty: continue
        plt.figure(figsize=(12, 6))
        df_plot = df_cluster.pivot(index='hhmm', columns='meter_id', values='P_kW_scaled')
        plt.plot(df_plot.index, df_plot.values, alpha=0.4, linewidth=0.8)

        df_promedio = df_cluster.groupby('hhmm')['P_kW_scaled'].mean()
        plt.plot(df_promedio.index, df_promedio.values, color='black', linewidth=5, label='Promedio del Cluster')
        plt.title(f"Cluster {i+1} - {temporada}")
        plt.xlabel("Hora del día (HH:MM)")
        plt.ylabel("Consumo (P_kW_scaled)")
        
        ticks_visibles = df_promedio.index[::8] # 8 intervalos de 15 min = 2 horas
        plt.xticks(ticks_visibles, rotation=45)
        
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        plt.tight_layout()
        plt.show()