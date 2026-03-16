-- ============================================================
-- INITIALISATION DE LA BASE DE DONNÉES OBRAIL
-- ============================================================
-- Ce script est exécuté automatiquement au démarrage du conteneur
-- PostgreSQL (via docker-entrypoint-initdb.d).
-- ============================================================

\i /docker-entrypoint-initdb.d/01-schema.sql
