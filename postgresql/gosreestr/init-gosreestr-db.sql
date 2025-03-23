-- -----------------------------------------------------
-- Схема нормализованной базы данных (3НФ)
-- Для хранения данных Госреестра и Аукционов
-- -----------------------------------------------------

-- Создание схем
CREATE SCHEMA IF NOT EXISTS reference;   -- Для справочников
CREATE SCHEMA IF NOT EXISTS gosreestr;   -- Для данных госреестра
CREATE SCHEMA IF NOT EXISTS auction;     -- Для данных аукционов

-- =============================================================
-- СПРАВОЧНИКИ (REFERENCE)
-- =============================================================

-- Организационно-правовые формы
CREATE TABLE reference.organization_types (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL UNIQUE,    -- ГУО, ТОО, и т.д.
    name_ru TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Отрасли (верхний уровень ОКЭД)
CREATE TABLE reference.industry_sectors (
    id SERIAL PRIMARY KEY,
    code VARCHAR(5) NOT NULL UNIQUE,     -- O, K, P, и т.д.
    name_ru TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Виды деятельности (ОКЭД L3)
CREATE TABLE reference.industry_activities (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL UNIQUE,    -- 8413, 6420, и т.д.
    name_ru TEXT NOT NULL,
    sector_id INTEGER REFERENCES reference.industry_sectors(id),
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Уровни KFS (классификатор форм собственности)
CREATE TABLE reference.kfs_levels (
    id SERIAL PRIMARY KEY,
    level INTEGER NOT NULL,              -- 0, 1, 2
    code VARCHAR(10) NOT NULL UNIQUE,    -- 2, 214, 214001, и т.д.
    name_ru TEXT NOT NULL,
    parent_id INTEGER REFERENCES reference.kfs_levels(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Статусы организаций
CREATE TABLE reference.organization_statuses (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL UNIQUE,    -- ACT, и т.д.
    name_ru TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Типы аукционов
CREATE TABLE reference.auction_types (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,    -- TenderArenda10082019, и т.д.
    name_ru TEXT NOT NULL,
    name_kz TEXT,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Статусы аукционов
CREATE TABLE reference.auction_statuses (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,    -- AcceptingApplications, и т.д.
    name_ru TEXT NOT NULL,
    name_kz TEXT,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Типы объектов аукционов
CREATE TABLE reference.object_types (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name_ru TEXT NOT NULL,
    name_kz TEXT,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Периоды оплаты
CREATE TABLE reference.payment_periods (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name_ru TEXT NOT NULL,
    name_kz TEXT,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Страны
CREATE TABLE reference.countries (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL UNIQUE,
    name_ru TEXT NOT NULL,
    name_kz TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Регионы (области)
CREATE TABLE reference.regions (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL UNIQUE,
    name_ru TEXT NOT NULL,
    name_kz TEXT,
    country_id INTEGER REFERENCES reference.countries(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Районы
CREATE TABLE reference.districts (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL UNIQUE,
    name_ru TEXT NOT NULL,
    name_kz TEXT,
    region_id INTEGER REFERENCES reference.regions(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =============================================================
-- ДАННЫЕ ГОСРЕЕСТРА (GOSREESTR)
-- =============================================================

-- Организации
CREATE TABLE gosreestr.organizations (
    id SERIAL PRIMARY KEY,
    bin VARCHAR(12) NOT NULL UNIQUE,
    name_ru TEXT NOT NULL,
    organization_type_id INTEGER REFERENCES reference.organization_types(id),
    industry_sector_id INTEGER REFERENCES reference.industry_sectors(id),
    industry_activity_id INTEGER REFERENCES reference.industry_activities(id),
    kfs_l0_id INTEGER REFERENCES reference.kfs_levels(id),
    kfs_l1_id INTEGER REFERENCES reference.kfs_levels(id),
    kfs_l2_id INTEGER REFERENCES reference.kfs_levels(id),
    state_involvement NUMERIC,
    status_id INTEGER REFERENCES reference.organization_statuses(id),
    owner_id INTEGER REFERENCES gosreestr.organizations(id),
    ogu_id INTEGER REFERENCES gosreestr.organizations(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =============================================================
-- ДАННЫЕ АУКЦИОНОВ (AUCTION)
-- =============================================================

-- Аукционы
CREATE TABLE auction.auctions (
    id SERIAL PRIMARY KEY,
    auction_id INTEGER NOT NULL UNIQUE,
    auction_type_id INTEGER REFERENCES reference.auction_types(id),
    auction_status_id INTEGER REFERENCES reference.auction_statuses(id),
    start_date TIMESTAMP,
    publish_date TIMESTAMP,
    start_price NUMERIC,
    min_price NUMERIC,
    guarantee_payment_amount NUMERIC,
    payment_period_id INTEGER REFERENCES reference.payment_periods(id),
    min_participants_count INTEGER,
    participants_count INTEGER,
    win_price NUMERIC,
    publish_note_ru TEXT,
    publish_note_kz TEXT,
    payments_recipient_info_ru TEXT,
    payments_recipient_info_kz TEXT,
    note_ru TEXT,
    note_kz TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Объекты аукционов
CREATE TABLE auction.objects (
    id SERIAL PRIMARY KEY,
    auction_id INTEGER NOT NULL REFERENCES auction.auctions(id),
    object_type_id INTEGER REFERENCES reference.object_types(id),
    name_ru TEXT,
    name_kz TEXT,
    description_ru TEXT,
    description_kz TEXT,
    balanceholder_info_ru TEXT,
    balanceholder_info_kz TEXT,
    seller_id INTEGER REFERENCES gosreestr.organizations(id),
    seller_xin VARCHAR(12),
    seller_info_ru TEXT,
    seller_info_kz TEXT,
    seller_phone_ru TEXT,
    seller_phone_kz TEXT,
    meta_data JSONB,
    sdu_date TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Адреса
CREATE TABLE gosreestr.addresses (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,    -- 'organization', 'auction_object', 'seller'
    entity_id INTEGER NOT NULL,
    country_id INTEGER REFERENCES reference.countries(id),
    region_id INTEGER REFERENCES reference.regions(id),
    district_id INTEGER REFERENCES reference.districts(id),
    address TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT addresses_entity_unique UNIQUE (entity_type, entity_id)
);

-- =============================================================
-- ИМПОРТ ДАННЫХ (DATA_IMPORTS)
-- =============================================================

-- Записи об импортах
CREATE TABLE public.data_imports (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,         -- Источник данных (gosreestr, auction)
    import_date TIMESTAMP NOT NULL DEFAULT NOW(),
    records_count INTEGER,                -- Количество загруженных записей
    status VARCHAR(50) NOT NULL,          -- Статус загрузки (success, error)
    error_message TEXT,                   -- Сообщение об ошибке (если есть)
    file_name VARCHAR(255),               -- Имя исходного файла
    additional_info JSONB                 -- Дополнительная информация
);

-- =============================================================
-- ПРЕДСТАВЛЕНИЯ (VIEWS)
-- =============================================================

-- Полные данные организаций
CREATE VIEW gosreestr.organizations_full AS
SELECT
    o.id,
    o.bin,
    o.name_ru,
    ot.code AS organization_type_code,
    ot.name_ru AS organization_type_name,
    is.code AS industry_sector_code,
    is.name_ru AS industry_sector_name,
    ia.code AS industry_activity_code,
    ia.name_ru AS industry_activity_name,
    o.state_involvement,
    os.code AS status_code,
    os.name_ru AS status_name,
    owner.bin AS owner_bin,
    owner.name_ru AS owner_name,
    ogu.bin AS ogu_bin,
    ogu.name_ru AS ogu_name,
    o.created_at,
    o.updated_at
FROM
    gosreestr.organizations o
LEFT JOIN
    reference.organization_types ot ON o.organization_type_id = ot.id
LEFT JOIN
    reference.industry_sectors is ON o.industry_sector_id = is.id
LEFT JOIN
    reference.industry_activities ia ON o.industry_activity_id = ia.id
LEFT JOIN
    reference.organization_statuses os ON o.status_id = os.id
LEFT JOIN
    gosreestr.organizations owner ON o.owner_id = owner.id
LEFT JOIN
    gosreestr.organizations ogu ON o.ogu_id = ogu.id;

-- Полные данные аукционов
CREATE VIEW auction.auctions_full AS
SELECT
    a.id,
    a.auction_id,
    at.code AS auction_type_code,
    at.name_ru AS auction_type_name,
    ast.code AS auction_status_code,
    ast.name_ru AS auction_status_name,
    a.start_date,
    a.publish_date,
    a.start_price,
    a.min_price,
    a.guarantee_payment_amount,
    pp.code AS payment_period_code,
    pp.name_ru AS payment_period_name,
    a.min_participants_count,
    a.participants_count,
    a.win_price,
    a.note_ru,
    a.note_kz,
    a.created_at,
    a.updated_at
FROM
    auction.auctions a
LEFT JOIN
    reference.auction_types at ON a.auction_type_id = at.id
LEFT JOIN
    reference.auction_statuses ast ON a.auction_status_id = ast.id
LEFT JOIN
    reference.payment_periods pp ON a.payment_period_id = pp.id;

-- Полные данные объектов аукционов
CREATE VIEW auction.objects_full AS
SELECT
    o.id,
    o.auction_id,
    a.auction_id AS original_auction_id,
    ot.code AS object_type_code,
    ot.name_ru AS object_type_name,
    o.name_ru,
    o.name_kz,
    o.description_ru,
    o.description_kz,
    o.balanceholder_info_ru,
    o.balanceholder_info_kz,
    seller.bin AS seller_bin,
    seller.name_ru AS seller_name,
    o.seller_xin,
    o.seller_info_ru,
    o.seller_info_kz,
    o.seller_phone_ru,
    o.seller_phone_kz,
    o.meta_data,
    o.created_at,
    o.updated_at
FROM
    auction.objects o
JOIN
    auction.auctions a ON o.auction_id = a.id
LEFT JOIN
    reference.object_types ot ON o.object_type_id = ot.id
LEFT JOIN
    gosreestr.organizations seller ON o.seller_id = seller.id;

-- =============================================================
-- ТРИГГЕРЫ для автоматического обновления updated_at
-- =============================================================

-- Триггерная функция
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггеры для всех таблиц
CREATE TRIGGER update_organization_types_modtime
    BEFORE UPDATE ON reference.organization_types
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_industry_sectors_modtime
    BEFORE UPDATE ON reference.industry_sectors
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_industry_activities_modtime
    BEFORE UPDATE ON reference.industry_activities
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_kfs_levels_modtime
    BEFORE UPDATE ON reference.kfs_levels
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_organization_statuses_modtime
    BEFORE UPDATE ON reference.organization_statuses
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_auction_types_modtime
    BEFORE UPDATE ON reference.auction_types
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_auction_statuses_modtime
    BEFORE UPDATE ON reference.auction_statuses
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_object_types_modtime
    BEFORE UPDATE ON reference.object_types
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_payment_periods_modtime
    BEFORE UPDATE ON reference.payment_periods
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_countries_modtime
    BEFORE UPDATE ON reference.countries
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_regions_modtime
    BEFORE UPDATE ON reference.regions
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_districts_modtime
    BEFORE UPDATE ON reference.districts
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_organizations_modtime
    BEFORE UPDATE ON gosreestr.organizations
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_auctions_modtime
    BEFORE UPDATE ON auction.auctions
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_objects_modtime
    BEFORE UPDATE ON auction.objects
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_addresses_modtime
    BEFORE UPDATE ON gosreestr.addresses
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- =============================================================
-- ПРАВА ДОСТУПА
-- =============================================================

-- Создание роли
CREATE ROLE data_manager WITH LOGIN PASSWORD 'strong_password_here';

-- Предоставление прав на схемы
GRANT USAGE ON SCHEMA reference TO data_manager;
GRANT USAGE ON SCHEMA gosreestr TO data_manager;
GRANT USAGE ON SCHEMA auction TO data_manager;

-- Предоставление прав на таблицы
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA reference TO data_manager;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gosreestr TO data_manager;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA auction TO data_manager;
GRANT SELECT, INSERT, UPDATE ON public.data_imports TO data_manager;

-- Предоставление прав на последовательности
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA reference TO data_manager;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gosreestr TO data_manager;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA auction TO data_manager;
GRANT USAGE, SELECT ON SEQUENCE public.data_imports_id_seq TO data_manager;