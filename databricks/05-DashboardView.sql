CREATE OR REPLACE VIEW workspace.liga.goles_por_equipo AS
SELECT equipo, SUM(goles) AS goles_marcados
FROM (
  SELECT equipo_local     AS equipo, goles_local     AS goles FROM workspace.liga.partidos
  UNION ALL
  SELECT equipo_visitante AS equipo, goles_visitante AS goles FROM workspace.liga.partidos
)
GROUP BY equipo
ORDER BY goles_marcados DESC;