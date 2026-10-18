-- Modifica los saldos: cada ejecución mueve otros 100 de la cuenta 1 a la 2.
BEGIN;
UPDATE sdpd.public.cuentas SET saldo = saldo - 100 WHERE id = 1;
UPDATE sdpd.public.cuentas SET saldo = saldo + 100 WHERE id = 2;
COMMIT;
SELECT * FROM sdpd.public.cuentas ORDER BY id;
SELECT sum(saldo) AS saldo_total FROM sdpd.public.cuentas;
