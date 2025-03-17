-- Создание схемы для данных госреестра
CREATE SCHEMA IF NOT EXISTS gosreestr;

-- Создание схемы для данных аукционов
CREATE SCHEMA IF NOT EXISTS auction;

-- Создание таблицы для объектов госреестра
CREATE TABLE gosreestr.objects (
    id SERIAL PRIMARY KEY,
    bin VARCHAR(12) NOT NULL UNIQUE,  -- БИН организации
    name_ru TEXT,              -- Наименование организации
    opf VARCHAR(100),          -- ОПФ организации
    oked_l0 VARCHAR(100),      -- Отрасль
    state_involvement NUMERIC, -- Гос участие, %
    status VARCHAR(50),        -- Статус объекта
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Создание индексов для повышения производительности
CREATE INDEX idx_gosreestr_objects_bin ON gosreestr.objects(bin);
CREATE INDEX idx_gosreestr_objects_status ON gosreestr.objects(status);
CREATE INDEX idx_gosreestr_objects_opf ON gosreestr.objects(opf);
CREATE INDEX idx_gosreestr_objects_oked_l0 ON gosreestr.objects(oked_l0);

-- Создание таблицы для аукционных торгов
CREATE TABLE auction.trades (
    id SERIAL PRIMARY KEY,
    auction_id INTEGER UNIQUE NOT NULL,   -- Номер торга (AuctionId)
    auction_type VARCHAR(100),            -- Тип торгов (AuctionType)
    start_date TIMESTAMP,                 -- Дата начала торгов (StartDate)
    publish_date TIMESTAMP,               -- Дата публикации (PublishDate)
    start_price NUMERIC,                  -- Стартовая цена (StartPrice)
    min_price NUMERIC,                    -- Минимальная цена (MinPrice)
    guarantee_payment_amount NUMERIC,     -- Гарантийный взнос (GuaranteePaymentAmount)
    pay_period VARCHAR(100),              -- Периодичность оплаты (PayPeriod)
    min_participants_count INTEGER,       -- Минимально требуемое кол-во участников (MinParticipantsCount)
    publish_note_ru TEXT,                 -- Данные о публикации (PublishNoteRu)
    publish_note_kz TEXT,                 -- Данные о публикации (PublishNoteKz)
    payments_recipient_info_ru TEXT,      -- Реквизиты получателя гар взноса (PaymentsRecipientInfoRu)
    payments_recipient_info_kz TEXT,      -- Реквизиты получателя гар взноса (PaymentsRecipientInfoKz)
    note_ru TEXT,                         -- Примечание (NoteRu)
    note_kz TEXT,                         -- Примечание (NoteKz)
    auction_status VARCHAR(100),          -- Статус торгов (AuctionStatus)
    win_price NUMERIC,                    -- Цена продажи/арендный платеж (WinPrice)
    participants_count INTEGER,           -- Кол-во участников (ParticipantsCount)
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Создание таблицы для объектов аукционных торгов
CREATE TABLE auction.objects (
    id SERIAL PRIMARY KEY,
    auction_id INTEGER NOT NULL,          -- Связь с торгами
    object_type VARCHAR(100),             -- Тип объекта (ObjectType)
    seller_xin VARCHAR(12),               -- БИН/ИИН продавца (SellerXin)
    seller_info_ru TEXT,                  -- Продавец (SellerInfoRu)
    seller_info_kz TEXT,                  -- Продавец (SellerInfoKz)
    balanceholder_info_ru TEXT,           -- Балансодержатель (BalanceholderInfoRu)
    balanceholder_info_kz TEXT,           -- Балансодержатель (BalanceholderInfoKz)
    name_ru TEXT,                         -- Наименование объекта (NameRu)
    name_kz TEXT,                         -- Наименование объекта (NameKz)
    description_ru TEXT,                  -- Описание объекта (DescriptionRu)
    description_kz TEXT,                  -- Описание объекта (DescriptionKz)
    seller_adr_country VARCHAR(100),      -- Адрес продавца страна (SellerAdrCountry)
    seller_adr_obl VARCHAR(100),          -- Адрес продавца область (SellerAdrObl)
    seller_adr_reg VARCHAR(100),          -- Адрес продавца район (SellerAdrReg)
    seller_adr_adr TEXT,                  -- Адрес продавца (SellerAdrAdr)
    seller_phone_ru TEXT,                 -- Телефон продавца (SellerPhoneRu)
    seller_phone_kz TEXT,                 -- Телефон продавца (SellerPhoneKz)
    object_adr_country VARCHAR(100),      -- Адрес объекта страна (ObjectAdrCountry)
    object_adr_obl VARCHAR(100),          -- Адрес объекта область (ObjectAdrObl)
    object_adr_reg VARCHAR(100),          -- Адрес объекта район (ObjectAdrReg)
    object_adr_adr TEXT,                  -- Адрес объекта (ObjectAdrAdr)
    meta_data JSONB,                      -- Метаданные (MetaData) в формате JSON
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Связь с таблицей торгов
    CONSTRAINT fk_auction_trade FOREIGN KEY (auction_id) 
        REFERENCES auction.trades(auction_id) ON DELETE CASCADE
);

-- Создание индексов для аукционных торгов
CREATE INDEX idx_auction_trades_auction_id ON auction.trades(auction_id);
CREATE INDEX idx_auction_trades_auction_status ON auction.trades(auction_status);
CREATE INDEX idx_auction_trades_start_date ON auction.trades(start_date);
CREATE INDEX idx_auction_trades_publish_date ON auction.trades(publish_date);

-- Создание индексов для объектов аукционных торгов
CREATE INDEX idx_auction_objects_auction_id ON auction.objects(auction_id);
CREATE INDEX idx_auction_objects_seller_xin ON auction.objects(seller_xin);
CREATE INDEX idx_auction_objects_object_type ON auction.objects(object_type);

-- Создание таблицы для хранения данных о последних загрузках
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

-- Создание представления для получения всех торгов с информацией об объектах
CREATE VIEW auction.trades_with_objects AS
SELECT 
    t.*,
    o.object_type,
    o.seller_xin,
    o.name_ru AS object_name_ru,
    o.name_kz AS object_name_kz,
    o.seller_info_ru,
    o.object_adr_country,
    o.object_adr_obl,
    o.object_adr_reg,
    o.object_adr_adr
FROM 
    auction.trades t
LEFT JOIN 
    auction.objects o ON t.auction_id = o.auction_id;

-- Добавление комментариев к таблицам для документации
COMMENT ON TABLE gosreestr.objects IS 'Объекты госреестра';
COMMENT ON TABLE auction.trades IS 'Аукционные торги';
COMMENT ON TABLE auction.objects IS 'Объекты аукционных торгов';
COMMENT ON TABLE public.data_imports IS 'История импорта данных';

-- Создание роли для работы с данными
CREATE ROLE data_manager WITH LOGIN PASSWORD 'strong_password_here';

-- Предоставление прав на созданные объекты
GRANT USAGE ON SCHEMA gosreestr TO data_manager;
GRANT USAGE ON SCHEMA auction TO data_manager;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gosreestr TO data_manager;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA auction TO data_manager;
GRANT SELECT, INSERT, UPDATE ON public.data_imports TO data_manager;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gosreestr TO data_manager;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA auction TO data_manager;
GRANT USAGE, SELECT ON SEQUENCE public.data_imports_id_seq TO data_manager;

-- Создание триггерной функции для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Добавление триггеров для автоматического обновления updated_at
CREATE TRIGGER update_gosreestr_objects_modtime
    BEFORE UPDATE ON gosreestr.objects
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_auction_trades_modtime
    BEFORE UPDATE ON auction.trades
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_auction_objects_modtime
    BEFORE UPDATE ON auction.objects
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();