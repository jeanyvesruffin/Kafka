-- Stock initial : insere UNIQUEMENT les produits absents.
--
-- La base H2 est un fichier, le script rejoue a chaque demarrage du service. Un MERGE ... KEY (product_id)
-- evite la cle dupliquee, mais il REECRIT aussi les lignes existantes : chaque redemarrage remettait le
-- stock a sa valeur initiale, effacant les reservations en cours (et la colonne version). Une commande
-- confirmee avant le redemarrage ne reservait alors plus rien : le stock pouvait etre vendu deux fois.
-- Ici, une ligne existante n'est jamais touchee.
MERGE INTO stock AS target
    USING (VALUES ('sku-001', 100),
                  ('sku-002', 50),
                  ('sku-003', 500),
                  ('sku-004', 10),
                  ('sku-005', 2)) AS seed (product_id, quantity)
    ON target.product_id = seed.product_id
    WHEN NOT MATCHED THEN
        INSERT (product_id, quantity_available, quantity_reserved, version)
        VALUES (seed.product_id, seed.quantity, 0, 0);
