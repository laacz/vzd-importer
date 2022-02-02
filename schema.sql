CREATE TABLE public.aw_ciems (
    code numeric(9,0) PRIMARY KEY,
    type numeric(3,0) NOT NULL,
    name character varying(128) NOT NULL,
    parent_code numeric(9,0) NOT NULL,
    parent_type numeric(3,0) NOT NULL,
    approved boolean,
    approve_degree numeric(3,0) DEFAULT NULL::numeric,
    status character(3) NOT NULL,
    sort_name character varying(516) NOT NULL,
    created_at date NOT NULL,
    modified_at date NOT NULL,
    deleted_at date,
    is_small boolean DEFAULT false NOT NULL,
    full_name character varying(256) NOT NULL,
    updated boolean DEFAULT false NOT NULL
);

CREATE TABLE public.aw_eka (
    code numeric(9,0) PRIMARY KEY,
    type numeric(3,0) NOT NULL,
    status character(3) NOT NULL,
    approved boolean,
    approve_degree numeric(3,0) DEFAULT NULL::numeric,
    parent_code numeric(9,0) NOT NULL,
    parent_type numeric(3,0) NOT NULL,
    name character varying(128) NOT NULL,
    sort_name character varying(516) NOT NULL,
    postal_code character(7) NOT NULL,
    postal_office_area_code numeric(9,0) NOT NULL,
    created_at date NOT NULL,
    modified_at date NOT NULL,
    deleted_at date,
    for_build boolean NOT NULL,
    planned_address boolean NOT NULL,
    full_name character varying(256) NOT NULL,
    x numeric(9,3) NOT NULL,
    y numeric(9,3) NOT NULL,
    lat numeric(9,7) NOT NULL,
    lng numeric(9,7) NOT NULL,
    geom public.geometry(Point,4326),
    updated boolean DEFAULT false NOT NULL
);

CREATE INDEX aw_eka_geom_idx ON public.aw_eka USING gist (geom);

CREATE TABLE public.aw_iela (
    code numeric(9,0) PRIMARY KEY,
    type numeric(3,0) NOT NULL,
    name character varying(128) NOT NULL,
    parent_code numeric(9,0) NOT NULL,
    parent_type numeric(3,0) NOT NULL,
    approved boolean,
    approve_degree numeric(3,0) DEFAULT NULL::numeric,
    status character(3) NOT NULL,
    sort_name character varying(516) NOT NULL,
    created_at date NOT NULL,
    modified_at date NOT NULL,
    deleted_at date,
    attr character varying(2) DEFAULT NULL::character varying,
    full_name character varying(256) NOT NULL,
    updated boolean DEFAULT false NOT NULL
);

CREATE TABLE public.aw_novads (
    code numeric(9,0) PRIMARY KEY,
    type numeric(3,0) NOT NULL,
    name character varying(128) NOT NULL,
    parent_code numeric(9,0) NOT NULL,
    parent_type numeric(3,0) NOT NULL,
    approved boolean,
    approve_degree numeric(3,0) DEFAULT NULL::numeric,
    status character(3) NOT NULL,
    sort_name character varying(516) NOT NULL,
    created_at date NOT NULL,
    modified_at date NOT NULL,
    deleted_at date,
    atvk character varying(32) NOT NULL,
    full_name character varying(256) NOT NULL,
    updated boolean DEFAULT false NOT NULL
);

CREATE TABLE public.aw_pagasts (
    code numeric(9,0) PRIMARY KEY,
    type numeric(3,0) NOT NULL,
    name character varying(128) NOT NULL,
    parent_code numeric(9,0) NOT NULL,
    parent_type numeric(3,0) NOT NULL,
    approved boolean,
    approve_degree numeric(3,0) DEFAULT NULL::numeric,
    status character(3) NOT NULL,
    sort_name character varying(516) NOT NULL,
    created_at date NOT NULL,
    modified_at date NOT NULL,
    deleted_at date,
    atvk character varying(32) NOT NULL,
    full_name character varying(256) NOT NULL,
    updated boolean DEFAULT false NOT NULL
);

CREATE TABLE public.aw_pilseta (
    code numeric(9,0) PRIMARY KEY,
    type numeric(3,0) NOT NULL,
    name character varying(128) NOT NULL,
    parent_code numeric(9,0) NOT NULL,
    parent_type numeric(3,0) NOT NULL,
    approved boolean,
    approve_degree numeric(3,0) DEFAULT NULL::numeric,
    status character(3) NOT NULL,
    sort_name character varying(516) NOT NULL,
    created_at date NOT NULL,
    modified_at date NOT NULL,
    deleted_at date,
    atvk character varying(32) NOT NULL,
    full_name character varying(256) NOT NULL,
    updated boolean DEFAULT false NOT NULL
);

DROP MATERIALIZED VIEW IF EXISTS public.aw_full_addresses;
CREATE MATERIALIZED VIEW aw_full_addresses
AS
SELECT e.code,
       e.name,
       iela.name AS iela_name,
       coalesce(
               ciems.code,
               ciems_no_ielas.code
           ) AS ciems_code,
       coalesce(
               ciems.name,
               ciems_no_ielas.name
           ) AS ciems_name,
       coalesce(
               pilseta.code,
               pilseta_no_ielas.code
           ) AS pilseta_code,
       coalesce(
               pilseta.name,
               pilseta_no_ielas.name
           ) AS pilseta_name,
       coalesce(
               pagasts.code,
               pagasts_no_ciema.code,
               pagasts_no_ciema_no_ielas.code
           ) AS pagasts_code,
       coalesce(
               pagasts.name,
               pagasts_no_ciema.name,
               pagasts_no_ciema_no_ielas.name
           ) AS pagasts_name,
       coalesce(
               novads_no_pagasta.code,
               novads_no_pagasta_no_ciema.code,
               novads_no_pagasta_no_ciema_no_ielas.code,
               novads_no_pilsetas.code,
               novads_no_pilsetas_no_ielas.code
           ) AS novads_code,
       coalesce(
               novads_no_pagasta.name,
               novads_no_pagasta_no_ciema.name,
               novads_no_pagasta_no_ciema_no_ielas.name,
               novads_no_pilsetas.name,
               novads_no_pilsetas_no_ielas.name
           ) AS novads_name,
       e.full_name,
       e.parent_code,
       e.parent_type,
       geom
FROM aw_eka e
         LEFT JOIN aw_iela iela ON iela.code = e.parent_code

    -- House is directly in a village
         LEFT JOIN aw_ciems ciems ON ciems.code = e.parent_code
    -- House is ON a street in a village
         LEFT JOIN aw_ciems ciems_no_ielas ON ciems_no_ielas.code = iela.parent_code

    -- House is directly in a city
         LEFT JOIN aw_pilseta pilseta ON pilseta.code = e.parent_code
    -- House is ON a street in a city
         LEFT JOIN aw_pilseta pilseta_no_ielas ON pilseta_no_ielas.code = iela.parent_code

    -- House is directly in a parish
         LEFT JOIN aw_pagasts pagasts ON pagasts.code = e.parent_code
    -- House is directly in a village in a parish
         LEFT JOIN aw_pagasts pagasts_no_ciema ON pagasts_no_ciema.code = ciems.parent_code
    -- House is ON a street in a village in a parish
         LEFT JOIN aw_pagasts pagasts_no_ciema_no_ielas
                   ON pagasts_no_ciema_no_ielas.code = ciems_no_ielas.parent_code
    -- [!] We do not have a town inside a parish. Parishes are for villages.

    -- House in in a parish in a county
         LEFT JOIN aw_novads novads_no_pagasta ON novads_no_pagasta.code = pagasts.parent_code
    -- House is in a village in a parish in a county
         LEFT JOIN aw_novads novads_no_pagasta_no_ciema
                   ON novads_no_pagasta_no_ciema.code = pagasts_no_ciema.parent_code
    -- House is ON a street in a village in a parish in a county
         LEFT JOIN aw_novads novads_no_pagasta_no_ciema_no_ielas
                   ON novads_no_pagasta_no_ciema_no_ielas.code = pagasts_no_ciema_no_ielas.parent_code
    -- House is directly in a town in a county
         LEFT JOIN aw_novads novads_no_pilsetas ON novads_no_pilsetas.code = pilseta.parent_code
    -- House is ON a street in a town in a county
         LEFT JOIN aw_novads novads_no_pilsetas_no_ielas
                   ON novads_no_pilsetas_no_ielas.code = pilseta_no_ielas.parent_code
     -- [!] We do not have any house which is directly in a county without an intermediate parish
     -- [!] We do not have a village which would be directly inside a county without a parish inbetween.
WHERE e.status = 'EKS';

CREATE INDEX aw_full_ads_geom_idx ON public.aw_full_addresses USING gist (geom);
