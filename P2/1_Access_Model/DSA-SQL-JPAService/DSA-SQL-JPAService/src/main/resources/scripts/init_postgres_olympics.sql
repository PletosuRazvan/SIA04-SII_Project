-- ============================================================
-- PostgreSQL: Create and populate Olympics noc_regions table
-- Run this in PostgreSQL (database: olympics)
-- ============================================================

-- Create database (run as superuser):
-- CREATE DATABASE olympics;

-- Create table
CREATE TABLE IF NOT EXISTS noc_regions (
    noc VARCHAR(5) PRIMARY KEY,
    region VARCHAR(100),
    notes VARCHAR(255)
);

-- Populate with Olympic NOC regions
INSERT INTO noc_regions (noc, region, notes) VALUES
('AFG', 'Afghanistan', NULL),
('ALB', 'Albania', NULL),
('ALG', 'Algeria', NULL),
('ARG', 'Argentina', NULL),
('AUS', 'Australia', NULL),
('AUT', 'Austria', NULL),
('BEL', 'Belgium', NULL),
('BRA', 'Brazil', NULL),
('CAN', 'Canada', NULL),
('CHN', 'China', NULL),
('CMR', 'Cameroon', NULL),
('COL', 'Colombia', NULL),
('CUB', 'Cuba', NULL),
('DEN', 'Denmark', NULL),
('EGY', 'Egypt', NULL),
('ESP', 'Spain', NULL),
('ETH', 'Ethiopia', NULL),
('FIN', 'Finland', NULL),
('FRA', 'France', NULL),
('GBR', 'UK', NULL),
('GER', 'Germany', NULL),
('GRE', 'Greece', NULL),
('HUN', 'Hungary', NULL),
('IND', 'India', NULL),
('IRL', 'Ireland', NULL),
('ITA', 'Italy', NULL),
('JAM', 'Jamaica', NULL),
('JPN', 'Japan', NULL),
('KEN', 'Kenya', NULL),
('KOR', 'South Korea', NULL),
('MEX', 'Mexico', NULL),
('NED', 'Netherlands', NULL),
('NOR', 'Norway', NULL),
('NZL', 'New Zealand', NULL),
('POL', 'Poland', NULL),
('POR', 'Portugal', NULL),
('ROU', 'Romania', NULL),
('RSA', 'South Africa', NULL),
('RUS', 'Russia', NULL),
('SRB', 'Serbia', NULL),
('SUI', 'Switzerland', NULL),
('SWE', 'Sweden', NULL),
('TUR', 'Turkey', NULL),
('UKR', 'Ukraine', NULL),
('URS', 'Soviet Union', NULL),
('USA', 'USA', NULL)
ON CONFLICT (noc) DO NOTHING;

