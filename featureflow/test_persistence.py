import unittest2
from persistence import simple_in_memory_settings
from bytestream import ByteStream, ByteStreamFeature
from feature import Feature
from model import BaseModel
from io import BytesIO


class SimpleInMemorySettingsDecoratorTests(unittest2.TestCase):
    def test_can_process_document_using_decorated_class(self):
        @simple_in_memory_settings
        class Document(BaseModel):
            stream = ByteStreamFeature(
                ByteStream,
                chunksize=128,
                store=True)

            uppercase = Feature(
                lambda x: x.upper(),
                needs=stream,
                store=False)

        input = BytesIO('Here is some text')
        _id = Document.process(stream=input)
        doc = Document(_id)
        self.assertEqual('HERE IS SOME TEXT', doc.uppercase.read())

    def test_decorated_class_has_correct_module_and_name(self):

        class Document(BaseModel):
            stream = ByteStreamFeature(
                ByteStream,
                chunksize=128,
                store=True)

            uppercase = Feature(
                lambda x: x.upper(),
                needs=stream,
                store=False)

        original_name = Document.__name__
        original_module = Document.__module__

        Document = simple_in_memory_settings(Document)

        self.assertEqual(original_name, Document.__name__)
        self.assertEqual(original_module, Document.__module__)
