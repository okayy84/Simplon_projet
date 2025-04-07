-- Stockage des résultats dans la BDD

-- chiffre d'affaire total
CREATE TABLE IF NOT EXISTS resultat_chiffre_affaire AS
SELECT SUM(tab.prix_ventes) AS total
FROM (
    SELECT v.id_vente, v.quantite, p.nom, p.prix, (v.quantite * p.prix) AS prix_ventes
    FROM ventes AS v
    LEFT JOIN produits AS p ON p.id_produit = v.id_produit
) AS tab;

-- ventes par produit
CREATE TABLE IF NOT EXISTS resultat_ventes_par_produit AS
SELECT v.id_produit, COUNT(v.id_vente) AS nombre_vente, SUM(v.quantite) AS somme
FROM ventes AS v
LEFT JOIN produits AS p ON p.id_produit = v.id_produit
GROUP BY v.id_produit
ORDER BY somme DESC;

-- ventes par ville
CREATE TABLE IF NOT EXISTS resultat_ventes_par_ville AS
SELECT ville, COUNT(v.id_vente) AS nombre_vente, SUM(v.quantite) AS somme
FROM ventes AS v
LEFT JOIN magasins AS m ON v.id_magasin = m.id_magasin
GROUP BY ville
ORDER BY nombre_vente DESC;