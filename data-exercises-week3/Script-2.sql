
INSERT INTO prediccion_fin_mundo (id_evento, nombre_evento, fecha_evento, descripcion_evento, dias_faltantes, fuente_prediccion)
SELECT
    id_evento,
    nombre_evento,
    fecha_evento,
    descripcion_evento,
    (fecha_evento - CURRENT_DATE) AS dias_faltantes,
    'NERF' AS fuente_prediccion
FROM eventos_apocalipticos;
