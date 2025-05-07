CREATE TABLE facture (
    numero SERIAL PRIMARY KEY,
    client VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    montant DECIMAL(10,2) NOT NULL
);

INSERT INTO facture (client, date, montant) VALUES
('Client1', CURRENT_DATE, 100.50),
('Client2', CURRENT_DATE, 200.75),
('Client3', CURRENT_DATE, 150.00);