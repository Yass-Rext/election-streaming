Les tables contiennent-elles des données ?

Tu as montré que les tables existent.

Maintenant exécute :

docker exec -it postgres-election psql -U postgres -d election

Puis :

SELECT COUNT(*) FROM resultats_regions;

SELECT COUNT(*) FROM resultats_candidats;

SELECT COUNT(*) FROM resultats_departements;