import matplotlib.pyplot as plt
from sklearn.cluster import KMeans

import os
os.environ["OMP_NUM_THREADS"] = "1" 

def graficar_codo(X_scaled, titulo):
    """
    Genera la gráfica del método del codo para determinar el k óptimo.
    """
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