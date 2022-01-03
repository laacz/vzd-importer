CREATE TABLE public.aw_ciems (
    code numeric(9,0) NOT NULL,
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
    code numeric(9,0) NOT NULL,
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

CREATE TABLE public.aw_iela (
    code numeric(9,0) NOT NULL,
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
    code numeric(9,0) NOT NULL,
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
    code numeric(9,0) NOT NULL,
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
    code numeric(9,0) NOT NULL,
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

ALTER TABLE ONLY public.aw_ciems
    ADD CONSTRAINT aw_ciems_pkey PRIMARY KEY (code);

ALTER TABLE ONLY public.aw_eka
    ADD CONSTRAINT aw_eka_pkey PRIMARY KEY (code);

ALTER TABLE ONLY public.aw_iela
    ADD CONSTRAINT aw_iela_pkey PRIMARY KEY (code);

ALTER TABLE ONLY public.aw_novads
    ADD CONSTRAINT aw_novads_pkey PRIMARY KEY (code);

ALTER TABLE ONLY public.aw_pagasts
    ADD CONSTRAINT aw_pagasts_pkey PRIMARY KEY (code);

ALTER TABLE ONLY public.aw_pilseta
    ADD CONSTRAINT aw_pilseta_pkey PRIMARY KEY (code);

CREATE INDEX aw_eka_geom_idx ON public.aw_eka USING gist (geom);
