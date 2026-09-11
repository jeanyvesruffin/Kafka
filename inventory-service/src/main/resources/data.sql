-- Stock initial. MERGE (et non INSERT) pour que le redemarrage du service
-- sur une base H2 fichier existante ne plante pas sur une cle dupliquee.
MERGE INTO stock (product_id, quantity_available, quantity_reserved, version) KEY (product_id) VALUES ('sku-001', 100,
                                                                                                       0, 0),
                                                                                                      ('sku-002', 50, 0,
                                                                                                       0),
                                                                                                      ('sku-003', 500,
                                                                                                       0, 0),
                                                                                                      ('sku-004', 10, 0,
                                                                                                       0),
                                                                                                      ('sku-005', 2, 0,
                                                                                                       0);
