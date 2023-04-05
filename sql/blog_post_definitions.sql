CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

/*
Drop all tables in the public schema.
*/
DROP TABLE IF EXISTS public.comments;
DROP TABLE IF EXISTS public.posts;
DROP TABLE IF EXISTS public.users;
DROP TABLE IF EXISTS public.companies;
DROP TABLE IF EXISTS public.addresses;

/*
Create all tables in the public schema.
*/
CREATE TABLE public.addresses (
    uuid UUID NOT NULL DEFAULT uuid_generate_v4(),
    street VARCHAR(255),
    suite VARCHAR(255),
    city VARCHAR(255),
    zipcode VARCHAR(255),
    geo_lat DOUBLE PRECISION,
    geo_lng DOUBLE PRECISION,
    PRIMARY KEY (uuid)
);

CREATE TABLE public.companies (
    uuid UUID NOT NULL DEFAULT uuid_generate_v4(),
    name VARCHAR(255),
    catchPhrase VARCHAR(255),
    bs VARCHAR(255),
    PRIMARY KEY (uuid)
);

CREATE TABLE public.users (
    id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    address_uuid UUID NOT NULL,
    phone VARCHAR(255),
    website VARCHAR(255),
    company_uuid UUID NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_address_uuid
        FOREIGN KEY (address_uuid)
        REFERENCES addresses(uuid)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT fk_company_uuid
        FOREIGN KEY (company_uuid)
        REFERENCES companies(uuid)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE public.posts (
	user_id BIGINT NOT NULL,
    id BIGINT NOT NULL,
    title VARCHAR(255),
    body TEXT,
    PRIMARY KEY (id),
    CONSTRAINT fk_user_id
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE public.comments (
    post_id BIGINT NOT NULL,
    id BIGINT NOT NULL,
    name VARCHAR(255),
    email VARCHAR(255),
    body TEXT,
    PRIMARY KEY (id),
    CONSTRAINT fk_post_id
        FOREIGN KEY (post_id)
        REFERENCES posts(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);
