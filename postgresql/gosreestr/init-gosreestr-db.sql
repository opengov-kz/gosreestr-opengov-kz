-- Создание схем
CREATE SCHEMA IF NOT EXISTS gosreestr;
CREATE SCHEMA IF NOT EXISTS auction;
CREATE SCHEMA IF NOT EXISTS reference;

-- Схема reference (Справочники)
CREATE TABLE reference.organization_types (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL
);

CREATE TABLE reference.industry_sectors (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL
);

CREATE TABLE reference.industry_activities (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL,
    sector_id INTEGER REFERENCES reference.industry_sectors(id)
);

CREATE TABLE reference.kfs_levels (
    id SERIAL PRIMARY KEY,
    level INTEGER NOT NULL,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL,
    parent_id INTEGER REFERENCES reference.kfs_levels(id)
);

CREATE TABLE reference.organization_statuses (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL
);

CREATE TABLE reference.auction_types (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL
);

CREATE TABLE reference.auction_statuses (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL
);

CREATE TABLE reference.object_types (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL
);

CREATE TABLE reference.payment_periods (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL
);

CREATE TABLE reference.countries (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL
);

CREATE TABLE reference.regions (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL,
    country_id INTEGER REFERENCES reference.countries(id)
);

CREATE TABLE reference.districts (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL,
    region_id INTEGER REFERENCES reference.regions(id)
);

-- Схема gosreestr (Организации)
CREATE TABLE gosreestr.organizations (
    id SERIAL PRIMARY KEY,
    bin VARCHAR(12) UNIQUE NOT NULL,
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

CREATE TABLE gosreestr.organization_contacts (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER NOT NULL REFERENCES gosreestr.organizations(id) ON DELETE CASCADE,
    contact_type VARCHAR(50) NOT NULL,
    contact_value TEXT NOT NULL,
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE gosreestr.addresses (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    entity_id INTEGER NOT NULL,
    address_type VARCHAR(50) NOT NULL,
    country_id INTEGER REFERENCES reference.countries(id),
    region_id INTEGER REFERENCES reference.regions(id),
    district_id INTEGER REFERENCES reference.districts(id),
    address TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (entity_type, entity_id, address_type)
);

-- Схема auction (Аукционы)
CREATE TABLE auction.auctions (
    id SERIAL PRIMARY KEY,
    auction_id INTEGER UNIQUE NOT NULL,
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
    note_ru TEXT,
    note_kz TEXT,
    publish_note_ru TEXT,
    publish_note_kz TEXT,
    payments_recipient_info_ru TEXT,
    payments_recipient_info_kz TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE auction.sellers (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER REFERENCES gosreestr.organizations(id),
    xin VARCHAR(12) UNIQUE NOT NULL,
    name_ru TEXT NOT NULL,
    name_kz TEXT,
    info_ru TEXT,
    info_kz TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE auction.seller_contacts (
    id SERIAL PRIMARY KEY,
    seller_id INTEGER NOT NULL REFERENCES auction.sellers(id) ON DELETE CASCADE,
    contact_type VARCHAR(50) NOT NULL,
    contact_value TEXT NOT NULL,
    language VARCHAR(2) NOT NULL,
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE auction.auction_objects (
    id SERIAL PRIMARY KEY,
    auction_id INTEGER NOT NULL REFERENCES auction.auctions(id) ON DELETE CASCADE,
    object_type_id INTEGER REFERENCES reference.object_types(id),
    name_ru TEXT,
    name_kz TEXT,
    description_ru TEXT,
    description_kz TEXT,
    seller_id INTEGER REFERENCES auction.sellers(id),
    balanceholder_info_ru TEXT,
    balanceholder_info_kz TEXT,
    meta_data JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Индексы для оптимизации
-- Индексы для справочников
CREATE INDEX idx_industry_activities_sector_id ON reference.industry_activities(sector_id);
CREATE INDEX idx_kfs_levels_parent_id ON reference.kfs_levels(parent_id);
CREATE INDEX idx_regions_country_id ON reference.regions(country_id);
CREATE INDEX idx_districts_region_id ON reference.districts(region_id);

-- Индексы для организаций
CREATE INDEX idx_organizations_organization_type_id ON gosreestr.organizations(organization_type_id);
CREATE INDEX idx_organizations_industry_sector_id ON gosreestr.organizations(industry_sector_id);
CREATE INDEX idx_organizations_industry_activity_id ON gosreestr.organizations(industry_activity_id);
CREATE INDEX idx_organizations_kfs_l0_id ON gosreestr.organizations(kfs_l0_id);
CREATE INDEX idx_organizations_kfs_l1_id ON gosreestr.organizations(kfs_l1_id);
CREATE INDEX idx_organizations_kfs_l2_id ON gosreestr.organizations(kfs_l2_id);
CREATE INDEX idx_organizations_status_id ON gosreestr.organizations(status_id);
CREATE INDEX idx_organizations_owner_id ON gosreestr.organizations(owner_id);
CREATE INDEX idx_organizations_ogu_id ON gosreestr.organizations(ogu_id);
CREATE INDEX idx_organizations_bin ON gosreestr.organizations(bin);

-- Индексы для контактов
CREATE INDEX idx_organization_contacts_organization_id ON gosreestr.organization_contacts(organization_id);

-- Индексы для адресов
CREATE INDEX idx_addresses_entity ON gosreestr.addresses(entity_type, entity_id);
CREATE INDEX idx_addresses_country_id ON gosreestr.addresses(country_id);
CREATE INDEX idx_addresses_region_id ON gosreestr.addresses(region_id);
CREATE INDEX idx_addresses_district_id ON gosreestr.addresses(district_id);

-- Индексы для аукционов
CREATE INDEX idx_auctions_auction_type_id ON auction.auctions(auction_type_id);
CREATE INDEX idx_auctions_auction_status_id ON auction.auctions(auction_status_id);
CREATE INDEX idx_auctions_payment_period_id ON auction.auctions(payment_period_id);
CREATE INDEX idx_auctions_auction_id ON auction.auctions(auction_id);
CREATE INDEX idx_auctions_start_date ON auction.auctions(start_date);
CREATE INDEX idx_auctions_publish_date ON auction.auctions(publish_date);

-- Индексы для продавцов
CREATE INDEX idx_sellers_organization_id ON auction.sellers(organization_id);
CREATE INDEX idx_sellers_xin ON auction.sellers(xin);

-- Индексы для контактов продавцов
CREATE INDEX idx_seller_contacts_seller_id ON auction.seller_contacts(seller_id);

-- Индексы для объектов аукционов
CREATE INDEX idx_auction_objects_auction_id ON auction.auction_objects(auction_id);
CREATE INDEX idx_auction_objects_object_type_id ON auction.auction_objects(object_type_id);
CREATE INDEX idx_auction_objects_seller_id ON auction.auction_objects(seller_id);

-- Таблица для отслеживания импорта данных
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

-- Представления для удобства работы
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
    kfs0.code AS kfs_l0_code,
    kfs0.name_ru AS kfs_l0_name,
    kfs1.code AS kfs_l1_code,
    kfs1.name_ru AS kfs_l1_name,
    kfs2.code AS kfs_l2_code,
    kfs2.name_ru AS kfs_l2_name,
    o.state_involvement,
    os.code AS status_code,
    os.name_ru AS status_name,
    owner.bin AS owner_bin,
    owner.name_ru AS owner_name,
    ogu.bin AS ogu_bin,
    ogu.name_ru AS ogu_name
FROM
    gosreestr.organizations o
LEFT JOIN reference.organization_types ot ON o.organization_type_id = ot.id
LEFT JOIN reference.industry_sectors is ON o.industry_sector_id = is.id
LEFT JOIN reference.industry_activities ia ON o.industry_activity_id = ia.id
LEFT JOIN reference.kfs_levels kfs0 ON o.kfs_l0_id = kfs0.id
LEFT JOIN reference.kfs_levels kfs1 ON o.kfs_l1_id = kfs1.id
LEFT JOIN reference.kfs_levels kfs2 ON o.kfs_l2_id = kfs2.id
LEFT JOIN reference.organization_statuses os ON o.status_id = os.id
LEFT JOIN gosreestr.organizations owner ON o.owner_id = owner.id
LEFT JOIN gosreestr.organizations ogu ON o.ogu_id = ogu.id;

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
    a.publish_note_ru,
    a.publish_note_kz,
    a.payments_recipient_info_ru,
    a.payments_recipient_info_kz
FROM
    auction.auctions a
LEFT JOIN reference.auction_types at ON a.auction_type_id = at.id
LEFT JOIN reference.auction_statuses ast ON a.auction_status_id = ast.id
LEFT JOIN reference.payment_periods pp ON a.payment_period_id = pp.id;

CREATE VIEW auction.auction_objects_full AS
SELECT
    ao.id,
    ao.auction_id,
    a.auction_id AS external_auction_id,
    ot.code AS object_type_code,
    ot.name_ru AS object_type_name,
    ao.name_ru,
    ao.name_kz,
    ao.description_ru,
    ao.description_kz,
    s.xin AS seller_xin,
    s.name_ru AS seller_name_ru,
    s.name_kz AS seller_name_kz,
    s.info_ru AS seller_info_ru,
    s.info_kz AS seller_info_kz,
    ao.balanceholder_info_ru,
    ao.balanceholder_info_kz,
    ao.meta_data
FROM
    auction.auction_objects ao
JOIN auction.auctions a ON ao.auction_id = a.id
LEFT JOIN reference.object_types ot ON ao.object_type_id = ot.id
LEFT JOIN auction.sellers s ON ao.seller_id = s.id;

-- Создание триггерной функции для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Добавление триггеров для автоматического обновления updated_at
-- Для организаций
CREATE TRIGGER update_organizations_modtime
    BEFORE UPDATE ON gosreestr.organizations
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_organization_contacts_modtime
    BEFORE UPDATE ON gosreestr.organization_contacts
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_addresses_modtime
    BEFORE UPDATE ON gosreestr.addresses
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

-- Для аукционов
CREATE TRIGGER update_auctions_modtime
    BEFORE UPDATE ON auction.auctions
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_sellers_modtime
    BEFORE UPDATE ON auction.sellers
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_seller_contacts_modtime
    BEFORE UPDATE ON auction.seller_contacts
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_auction_objects_modtime
    BEFORE UPDATE ON auction.auction_objects
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

-- Добавление начальных данных в справочники
-- Организационно-правовые формы
INSERT INTO reference.organization_types (code, name_ru) VALUES
('ГУН', 'Государственное учреждение'),
('ГУО', 'Государственное учреждение образования');

-- Статусы организаций
INSERT INTO reference.organization_statuses (code, name_ru) VALUES
('ACT', 'Активный'),
('LIQ', 'Ликвидированный');

-- Отрасли
INSERT INTO reference.industry_sectors (code, name_ru) VALUES
('O', 'Государственное управление'),
('P', 'Образование'),
('R', 'Искусство, развлечения и отдых');

-- Виды деятельности
INSERT INTO reference.industry_activities (code, name_ru, sector_id) VALUES
('8423', 'Деятельность в области юстиции и правосудия', (SELECT id FROM reference.industry_sectors WHERE code = 'O')),
('8559', 'Прочие виды образования', (SELECT id FROM reference.industry_sectors WHERE code = 'P')),
('9312', 'Деятельность спортивных клубов', (SELECT id FROM reference.industry_sectors WHERE code = 'R'));

-- Уровни КФС
INSERT INTO reference.kfs_levels (level, code, name_ru, parent_id) VALUES
(0, '2', 'Республиканская собственность', NULL),
(0, '4', 'Коммунальная собственность', NULL);

INSERT INTO reference.kfs_levels (level, code, name_ru, parent_id) VALUES
(1, '242', 'Республиканские юридические лица', (SELECT id FROM reference.kfs_levels WHERE level = 0 AND code = '2')),
(1, '441', 'Коммунальные юридические лица областных акиматов', (SELECT id FROM reference.kfs_levels WHERE level = 0 AND code = '4'));

INSERT INTO reference.kfs_levels (level, code, name_ru, parent_id) VALUES
(2, '242001', 'Республиканские юридические лица на праве оперативного управления (казенные предприятия)', (SELECT id FROM reference.kfs_levels WHERE level = 1 AND code = '242')),
(2, '441118', 'Коммунальные юридические лица на праве хозяйственного ведения районных акиматов', (SELECT id FROM reference.kfs_levels WHERE level = 1 AND code = '441'));

-- Типы аукционов
INSERT INTO reference.auction_types (code, name_ru) VALUES
('TenderArenda10082019', 'Торги по аренде'),
('AuctionSale', 'Торги по продаже');

-- Статусы аукционов
INSERT INTO reference.auction_statuses (code, name_ru) VALUES
('AcceptingApplications', 'Прием заявок'),
('Scheduled', 'Запланирован'),
('Completed', 'Завершен'),
('Cancelled', 'Отменен');

-- Типы объектов
INSERT INTO reference.object_types (code, name_ru) VALUES
('Building', 'Здание'),
('Land', 'Земельный участок'),
('Equipment', 'Оборудование');

-- Периоды оплаты
INSERT INTO reference.payment_periods (code, name_ru) VALUES
('Monthly', 'Ежемесячно'),
('Quarterly', 'Ежеквартально'),
('Annually', 'Ежегодно');

-- Страны
INSERT INTO reference.countries (code, name_ru) VALUES
('KZ', 'Казахстан');

-- Создание роли для работы с данными
CREATE ROLE data_manager WITH LOGIN PASSWORD 'strong_password_here';

-- Предоставление прав на созданные объекты
GRANT USAGE ON SCHEMA gosreestr TO data_manager;
GRANT USAGE ON SCHEMA auction TO data_manager;
GRANT USAGE ON SCHEMA reference TO data_manager;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gosreestr TO data_manager;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA auction TO data_manager;
GRANT SELECT ON ALL TABLES IN SCHEMA reference TO data_manager;
GRANT SELECT, INSERT, UPDATE ON public.data_imports TO data_manager;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gosreestr TO data_manager;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA auction TO data_manager;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA reference TO data_manager;
GRANT USAGE, SELECT ON SEQUENCE public.data_imports_id_seq TO data_manager;