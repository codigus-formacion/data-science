-- Inicialización repetible: no sobrescribe los cambios hechos durante la clase.
CREATE DATABASE IF NOT EXISTS sdpd;
CREATE TABLE IF NOT EXISTS sdpd.public.pedidos (
    id INT PRIMARY KEY,
    cliente STRING NOT NULL,
    importe DECIMAL(10,2) NOT NULL,
    estado STRING NOT NULL
);
INSERT INTO sdpd.public.pedidos VALUES
    (1, 'Ana', 42.50, 'pendiente'),
    (2, 'Luis', 18.00, 'pendiente')
ON CONFLICT (id) DO NOTHING;
CREATE TABLE IF NOT EXISTS sdpd.public.cuentas (
    id INT PRIMARY KEY,
    saldo DECIMAL(12,2) NOT NULL CHECK (saldo >= 0)
);
INSERT INTO sdpd.public.cuentas VALUES (1, 1000.00), (2, 500.00)
ON CONFLICT (id) DO NOTHING;
-- Declarar el objetivo de replicación del despliegue de forma explícita.
ALTER DATABASE sdpd CONFIGURE ZONE USING num_replicas = 3;
