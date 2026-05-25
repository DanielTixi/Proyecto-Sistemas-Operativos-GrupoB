#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h> 
#include <time.h>
#include <unistd.h>
#include <string.h>

//Constantes globales del sistema

#define N 100000            
#define NUM_HILOS 3  
#define MAX_COLA 10000     
#define GENEROS 5


//Catologo generos de pelicula
typedef enum {
    GENERO_DESCONOCIDO = 0,
    DRAMA = 1,
    CIENCIA_FICCION = 2,
    COMEDIA = 3,
    TERROR = 4,
    DOCUMENTAL = 5
} GeneroNetflix;

// Registro de cada reproduccion 
typedef struct {
    int id_reproduccion;
    float porcentaje_visto;
    int genero_pelicula;
} RegistroNetflix;


// Registro de transmision enviados a pantalla
typedef struct {
    int id_registro;
    int id_hilo;
    char estado[30]; 
    float valor_porcentaje;
    int genero_final;
} ReporteTransmision;


// Configuracion de hilo
typedef struct {
    int id_hilo;
    int inicio;
    int fin;
    int moda_global;
    float min_visto;
    float max_visto;
    float promedio_visto;
    double tiempo_ejecucion;
} DatosHilo;

// VARIABLES GLOBALES
RegistroNetflix dataset[N];
ReporteTransmision cola_pantalla[MAX_COLA];

int frente_cola = 0;
int fin_cola = 0;
int hilos_activos = NUM_HILOS;

int registros_limpiados_global = 0;
int categorias_imputadas_global = 0;

// Mecanismos de sincronizacion 
pthread_mutex_t cerrojo_contador;
pthread_mutex_t cerrojo_cola;
sem_t elementos_cola;  
sem_t espacios_cola;   

// Generacion aleatoria de los datos de reproduccion en la memoria RAM
void generar_datos_netflix() {
    srand(time(NULL));
    for (int i = 0; i < N; i++) {
        dataset[i].id_reproduccion = i + 1;

        if (rand() % 10 == 0) {
            dataset[i].porcentaje_visto = -15.0; // 10% de errores
        } else {
            dataset[i].porcentaje_visto = (float)(rand() % 100); 
        }

        int prob = rand() % 100;
        if (prob < 15) {
            dataset[i].genero_pelicula = GENERO_DESCONOCIDO; 
        } else if (prob < 75) {
            dataset[i].genero_pelicula = CIENCIA_FICCION; // La Moda
        } else {
            dataset[i].genero_pelicula = (rand() % 4) + 1; 
            if (dataset[i].genero_pelicula >= 2) dataset[i].genero_pelicula++; 
        }
    }
    printf("SISTEMA: 100,000 registros creados en la memoria RAM.\n\n");
}

// Calculo estadistico del genero cinematografico mas repetido o moda global
int calcular_moda_netflix() {
    int conteo[GENEROS + 1] = {
        0};
    for (int i = 0; i < N; i++) {
        if (dataset[i].genero_pelicula != GENERO_DESCONOCIDO) conteo[dataset[i].genero_pelicula]++;
    }
    int moda = 1;
    for (int i = 2; i <= GENEROS; i++) {
        if (conteo[i] > conteo[moda]) moda = i;
    }
    return moda;
}

// Obtencion de los valores maximos minimos y promedio del porcentaje visualizado
void calcular_metricas_netflix(float *max, float *min, float *promedio) {
    float suma = 0;
    int cont_validos = 0;
    *max = -1e9; *min = 1e9;
    for (int i = 0; i < N; i++) {
        if (dataset[i].porcentaje_visto >= 0) {
            suma += dataset[i].porcentaje_visto;
            cont_validos++;
            if (dataset[i].porcentaje_visto > *max) *max = dataset[i].porcentaje_visto;
            if (dataset[i].porcentaje_visto < *min) *min = dataset[i].porcentaje_visto;
        }
    }
    *promedio = (cont_validos > 0) ? (suma / cont_validos) : 50.0;
}


// Procesamiento secuencial en un unico hilo 
double ejecutar_procesamiento_secuencial(int moda, float min, float max, float prom) {
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);


    RegistroNetflix* copia_temporal = malloc(N * sizeof(RegistroNetflix));
    memcpy(copia_temporal, dataset, N * sizeof(RegistroNetflix));

    for (int i = 0; i < N; i++) {
      
        if (copia_temporal[i].porcentaje_visto < 0) {
            copia_temporal[i].porcentaje_visto = prom;
        }
 
        else if (copia_temporal[i].genero_pelicula == 0) {
            copia_temporal[i].genero_pelicula = moda;
        }

        copia_temporal[i].porcentaje_visto = (copia_temporal[i].porcentaje_visto - min) / (max - min);
    }

    free(copia_temporal);
    clock_gettime(CLOCK_MONOTONIC, &end);
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

// Limpieza, imputacion y normalizacion concurrente de un bloque especifico de datos por parte de un hilo
void* procesar_bloque_netflix(void* arg) {
    DatosHilo* datos = (DatosHilo*)arg;
    int limpiados_local = 0;
    int imputados_local = 0;

    struct timespec hilo_start, hilo_end;
    clock_gettime(CLOCK_MONOTONIC, &hilo_start);

    for (int i = datos->inicio; i < datos->fin; i++) {
        char tag_estado[30] = "DATO SANO [OK]";

        if (dataset[i].porcentaje_visto < 0) {
            dataset[i].porcentaje_visto = datos->promedio_visto;
            limpiados_local++;
            strcpy(tag_estado, "ERR: Pct Negativo");
        }
        else if (dataset[i].genero_pelicula == GENERO_DESCONOCIDO) {
            dataset[i].genero_pelicula = datos->moda_global;
            imputados_local++;
            strcpy(tag_estado, "ERR: Gen Huerfano");
        }

        dataset[i].porcentaje_visto = (dataset[i].porcentaje_visto - datos->min_visto) / 
                                      (datos->max_visto - datos->min_visto);

        // Entrada segura a la cola circular intermedio RAM
        sem_wait(&espacios_cola);          
        pthread_mutex_lock(&cerrojo_cola); 

        cola_pantalla[fin_cola].id_registro = dataset[i].id_reproduccion;
        cola_pantalla[fin_cola].id_hilo = datos->id_hilo;
        strcpy(cola_pantalla[fin_cola].estado, tag_estado);
        cola_pantalla[fin_cola].valor_porcentaje = dataset[i].porcentaje_visto; 
        cola_pantalla[fin_cola].genero_final = dataset[i].genero_pelicula;
        
        fin_cola = (fin_cola + 1) % MAX_COLA; 

        pthread_mutex_unlock(&cerrojo_cola);
        sem_post(&elementos_cola);         
    }

    // Cronometrar salida del hilo trabajador
    clock_gettime(CLOCK_MONOTONIC, &hilo_end);
    datos->tiempo_ejecucion = (hilo_end.tv_sec - hilo_start.tv_sec) + 
                              (hilo_end.tv_nsec - hilo_start.tv_nsec) / 1e9;

    pthread_mutex_lock(&cerrojo_contador);
    registros_limpiados_global += limpiados_local;
    categorias_imputadas_global += imputados_local;
    hilos_activos--;
    pthread_mutex_unlock(&cerrojo_contador);

    pthread_exit(NULL);
}

// Extraccion de reportes desde la cola para mostrar registros normales en la consola principal y alertas de error en la terminal secundaria
void* hilo_monitoreo_completo(void* arg) {
    (void)arg;
    int items_procesados = 0;

    // Conexión dinámica a la Terminal 2
    FILE* terminal_errores = fopen("/dev/tty2", "w"); 
    if (terminal_errores == NULL) {
        terminal_errores = stdout; 
    } else {
        fprintf(terminal_errores, "=== PANTALLA SECUNDARIA: DETECCIÓN DE ANOMALÍAS EN VIVO ===\n\n");
    }

    while (items_procesados < N) {
        sem_wait(&elementos_cola);          
        pthread_mutex_lock(&cerrojo_cola);   

        ReporteTransmision item = cola_pantalla[frente_cola];
        frente_cola = (frente_cola + 1) % MAX_COLA;

        pthread_mutex_unlock(&cerrojo_cola);
        sem_post(&espacios_cola);           

        if (strstr(item.estado, "ERR") != NULL) {
            fprintf(terminal_errores, "[ALERTA] ID %d -> Procesado por Hilo %d | Condición: %s\n",
                    item.id_registro, item.id_hilo, item.estado);
            fflush(terminal_errores); 
        } else {
            printf("[SANO] Registro %d/100000 | Hilo %d | Pct Normalizado: %.4f | Género: %d\n",
                   item.id_registro, item.id_hilo, item.valor_porcentaje, item.genero_final);
            fflush(stdout); 
        }

        items_procesados++;
    }

    if (terminal_errores != stdout) {
        fprintf(terminal_errores, "\n[SISTEMA] Auditoría concluida con éxito.\n");
        fclose(terminal_errores);
    }
    pthread_exit(NULL);
}

// Hilo principal encargado de inicializar herramientas POSIX coordinar el despliegue del pipeline y presentar el reporte final de rendimiento
int main() {
    struct timespec start, end;
    generar_datos_netflix();

    float max_v, min_v, prom_v;
    calcular_metricas_netflix(&max_v, &min_v, &prom_v);
    int netflix_moda = calcular_moda_netflix();

    // 1. Ejecutar y capturar la métrica base secuencial real en la RAM
    printf("[SISTEMA] Evaluando rendimiento base secuencial en RAM...\n");
    double tiempo_secuencial_real = ejecutar_procesamiento_secuencial(netflix_moda, min_v, max_v, prom_v);
    printf("[SISTEMA] Prueba secuencial terminada.\n\n");

    // Inicialización de herramientas POSIX
    if (pthread_mutex_init(&cerrojo_contador, NULL) != 0 || pthread_mutex_init(&cerrojo_cola, NULL) != 0) {
        perror("[ERR] Fallo Mutex"); exit(EXIT_FAILURE);
    }
    if (sem_init(&elementos_cola, 0, 0) != 0 || sem_init(&espacios_cola, 0, MAX_COLA) != 0) {
        perror("[ERR] Fallo Semáforos"); exit(EXIT_FAILURE);
    }

    pthread_t hilos_trabajadores[NUM_HILOS];
    pthread_t hilo_monitor;
    DatosHilo configuracion_hilos[NUM_HILOS];
    int tam_bloque = N / NUM_HILOS;

    printf("[SISTEMA OPERATIVO] Desplegando Pipeline Mixto Concurrente...\n");
    clock_gettime(CLOCK_MONOTONIC, &start);

    pthread_create(&hilo_monitor, NULL, hilo_monitoreo_completo, NULL);

    for (int i = 0; i < NUM_HILOS; i++) {
        configuracion_hilos[i].id_hilo = i + 1;
        configuracion_hilos[i].inicio = i * tam_bloque;
        configuracion_hilos[i].fin = (i == NUM_HILOS - 1) ? N : (i + 1) * tam_bloque;
        configuracion_hilos[i].moda_global = netflix_moda;
        configuracion_hilos[i].min_visto = min_v;
        configuracion_hilos[i].max_visto = max_v;
        configuracion_hilos[i].promedio_visto = prom_v;
        configuracion_hilos[i].tiempo_ejecucion = 0.0;

        pthread_create(&hilos_trabajadores[i], NULL, procesar_bloque_netflix, &configuracion_hilos[i]);
    }

    for (int i = 0; i < NUM_HILOS; i++) {
        pthread_join(hilos_trabajadores[i], NULL);
    }
    pthread_join(hilo_monitor, NULL); 

    clock_gettime(CLOCK_MONOTONIC, &end);
    double tiempo_total_concurrente = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;

    // Destrucción de recursos del sistema operativo
    pthread_mutex_destroy(&cerrojo_contador);
    pthread_mutex_destroy(&cerrojo_cola);
    sem_destroy(&elementos_cola);
    sem_destroy(&espacios_cola);

    // Cálculo del Factor de Aceleración Secuencial RAM vs Hilos Totales
    double tiempo_secuencial_acumulado = 0.0;
    for(int i=0; i<NUM_HILOS; i++) {
        tiempo_secuencial_acumulado += configuracion_hilos[i].tiempo_ejecucion;
    }
    double speedup_teorico = tiempo_secuencial_acumulado / tiempo_total_concurrente;

    // =========================================================================
    // PRESENTACIÓN FINAL DE RESULTADOS COMPLETA
    // =========================================================================
    printf("\n=============================================================\n");
    printf(" REPORTES DE AUDITORÍA Y RENDIMIENTO CONCURRENTE\n");
    printf("=============================================================\n");
    printf("  Tiempos de ejecución por Hilo Trabajador:\n");
    for (int i = 0; i < NUM_HILOS; i++) {
        printf("   [➔] Hilo %d: %.5f segundos (Procesó %d datos)\n", 
               configuracion_hilos[i].id_hilo, 
               configuracion_hilos[i].tiempo_ejecucion,
               (configuracion_hilos[i].fin - configuracion_hilos[i].inicio));
    }
    printf("-------------------------------------------------------------\n");
    printf(" Tiempo Secuencial Puro (Cómputo Neto en RAM): %.5f segs\n", tiempo_secuencial_real);
    printf(" Tiempo Secuencial Acumulado (Con Overhead):  %.5f segs\n", tiempo_secuencial_acumulado);
    printf(" Tiempo Concurrente Real (Impresión + Pipeline): %.5f segs\n", tiempo_total_concurrente);
    printf(" Eficiencia del Pipeline de Despliegue:          %.2fx\n", speedup_teorico);
    printf("-------------------------------------------------------------\n");
    printf(" Total de porcentajes negativos saneados: %d\n", registros_limpiados_global);
    printf(" Total de géneros imputados (Moda %d):   %d\n", netflix_moda, categorias_imputadas_global);
    printf("=============================================================\n");

    return 0;
}