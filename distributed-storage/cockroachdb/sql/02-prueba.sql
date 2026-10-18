-- Una prueba independiente que no modifica los saldos del ejercicio.
CREATE TABLE IF NOT EXISTS sdpd.public.verificacion (
    id INT PRIMARY KEY,
    saldo INT NOT NULL
);
UPSERT INTO sdpd.public.verificacion VALUES (1, 1000), (2, 500);
BEGIN;
UPDATE sdpd.public.verificacion SET saldo = saldo - 100 WHERE id = 1;
UPDATE sdpd.public.verificacion SET saldo = saldo + 100 WHERE id = 2;
-- Provocar error SQL si la transferencia no dejó los valores esperados.
SELECT 1 / CASE WHEN
    (SELECT saldo FROM sdpd.public.verificacion WHERE id = 1) = 900
    AND (SELECT saldo FROM sdpd.public.verificacion WHERE id = 2) = 600
    AND (SELECT sum(saldo) FROM sdpd.public.verificacion) = 1500
    THEN 1 ELSE 0 END AS transferencia_correcta;
ROLLBACK;
SELECT 1 / CASE WHEN
    (SELECT saldo FROM sdpd.public.verificacion WHERE id = 1) = 1000
    AND (SELECT saldo FROM sdpd.public.verificacion WHERE id = 2) = 500
    THEN 1 ELSE 0 END AS rollback_correcto;
SELECT * FROM sdpd.public.verificacion ORDER BY id;
