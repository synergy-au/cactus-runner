INSERT INTO public.site("site_id", "nmi", "aggregator_id", "timezone_id", "created_time", "changed_time", "lfdi", "sfdi", "device_category") VALUES (1, '1111111111', 1, 'Australia/Brisbane', '2000-01-01 00:00:00Z', '2022-02-03 04:05:06.500', '5b3be900b754e7e6d2dc592170e50ee29ae4e48d', 244904468595, 0);

SELECT pg_catalog.setval('public.site_site_id_seq', 1, true);


INSERT INTO public.site_der("site_der_id", "created_time", "changed_time", "site_id") 
VALUES (1, '2000-01-01 00:00:00Z', '2024-03-14 04:55:44.500', 1);

SELECT pg_catalog.setval('public.site_der_site_der_id_seq', 1, true);
