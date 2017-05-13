import unittest2
from eventlog import InMemoryChannel, EventLog
from model import BaseModel
from persistence import PersistenceSettings
import tempfile
import shutil
from test_integration import TextStream, ToUpper
from feature import Feature
from data import UuidProvider, StringDelimitedKeyBuilder, InMemoryDatabase
import json


class EventLogTests(unittest2.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp()

        class Settings(PersistenceSettings):
            id_provider = UuidProvider()
            key_builder = StringDelimitedKeyBuilder()
            database = InMemoryDatabase(key_builder=key_builder)
            event_log = EventLog(path=self._dir, channel=InMemoryChannel())

        self.Settings = Settings

        class Model(BaseModel, Settings):
            stream = Feature(TextStream, store=True)
            upper = Feature(ToUpper, needs=stream, store=True)

        self.Model = Model

    def tearDown(self):
        shutil.rmtree(self._dir)

    def _fetch(self, subscription):
        _, raw = subscription.next()
        return json.loads(raw)

    def test_event_log_count_zero_documents(self):
        self.assertEqual(0, self.Settings.event_log.__len__())

    def test_event_log_count_one_document(self):
        self.Model.process(stream='Bah bah black sheep')
        self.assertEqual(2, self.Settings.event_log.__len__())

    def test_event_log_count_two_documents(self):
        self.Model.process(stream='Bah bah black sheep')
        self.Model.process(stream='Bah bah black sheep')
        self.assertEqual(4, self.Settings.event_log.__len__())

    def test_events_are_logged_when_event_log_is_configured(self):

        d1 = self.Model.process(stream='Bah bah black sheep')
        d2 = self.Model.process(stream='Humpty dumpty sat on a wall')

        subscription = self.Settings.event_log.subscribe(
            last_id='', raise_when_empty=True)

        e1 = self._fetch(subscription)
        e2 = self._fetch(subscription)
        e3 = self._fetch(subscription)
        e4 = self._fetch(subscription)

        self.assertEqual(d1, e1['_id'])
        self.assertEqual('upper', e1['name'])
        self.assertEqual(d1, e2['_id'])
        self.assertEqual('stream', e2['name'])

        self.assertEqual(d2, e3['_id'])
        self.assertEqual('upper', e3['name'])
        self.assertEqual(d2, e4['_id'])
        self.assertEqual('stream', e4['name'])

    def test_can_receive_events_as_they_occur(self):
        subscription = self.Settings.event_log.subscribe(
            last_id='', raise_when_empty=True)

        d1 = self.Model.process(stream='Bah bah black sheep')
        e1 = self._fetch(subscription)
        e2 = self._fetch(subscription)
        self.assertEqual(d1, e1['_id'])
        self.assertEqual('upper', e1['name'])
        self.assertEqual(d1, e2['_id'])
        self.assertEqual('stream', e2['name'])

        d2 = self.Model.process(stream='Humpty dumpty sat on a wall')
        e3 = self._fetch(subscription)
        e4 = self._fetch(subscription)
        self.assertEqual(d2, e3['_id'])
        self.assertEqual('upper', e3['name'])
        self.assertEqual(d2, e4['_id'])
        self.assertEqual('stream', e4['name'])
