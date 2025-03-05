SET row_security = off;
SET timezone = 'UTC';

-- TRUNCATE public.site_der, public.site, public.dynamic_operating_envelope, public.aggregator_certificate_assignment, public.certificate, public.aggregator CASCADE;

-- SELECT pg_catalog.setval('public.aggregator_aggregator_id_seq', 0, true);
-- SELECT pg_catalog.setval('public.certificate_certificate_id_seq', 0, true);

-- ALTER SEQUENCE public.aggregator_aggregator_id_seq RESTART WITH 1;
-- ALTER SEQUENCE public.aggregator_certificate_assignment_assignment_id_seq RESTART WITH 1;
-- ALTER SEQUENCE public.certificate_certificate_id_seq RESTART WITH 1;
-- ALTER SEQUENCE public.dynamic_operating_envelope_dynamic_operating_envelope_id_seq RESTART WITH 1;
-- ALTER SEQUENCE public.site_der_site_der_id_seq RESTART WITH 1;
-- ALTER SEQUENCE public.site_site_id_seq RESTART WITH 1;

TRUNCATE TABLE public.site_der RESTART IDENTITY CASCADE;
TRUNCATE TABLE public.site RESTART IDENTITY CASCADE;
TRUNCATE TABLE public.dynamic_operating_envelope RESTART IDENTITY CASCADE;
TRUNCATE TABLE public.aggregator_certificate_assignment RESTART IDENTITY CASCADE;
TRUNCATE TABLE public.certificate RESTART IDENTITY CASCADE;
TRUNCATE TABLE public.aggregator RESTART IDENTITY CASCADE;
