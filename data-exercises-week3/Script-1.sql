select * from eventos_apocalipticos ea ;

# Creacion tabla destino:
CREATE TABLE prediccion_fin_mundo (
    id_evento INT,
    nombre_evento VARCHAR(100),
    fecha_evento DATE,
    descripcion_evento VARCHAR(500),
    dias_faltantes INT,
    fuente_prediccion VARCHAR(100)
);

