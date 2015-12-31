from data import UuidProvider, StringDelimitedKeyBuilder, InMemoryDatabase


class PersistenceSettings(object):
    id_provider = UuidProvider()
    key_builder = StringDelimitedKeyBuilder()
    database = InMemoryDatabase(key_builder=key_builder)

    @classmethod
    def clone(cls, id_provider=None, key_builder=None, database=None):
        ip = id_provider
        kb = key_builder
        db = database

        class Settings(PersistenceSettings):
            id_provider = ip or cls.id_provider
            key_builder = kb or cls.key_builder
            database = db or cls.database

        return Settings
