from data import Database
import requests
import json
from io import BytesIO
import httplib
import urllib


class WriteStream(object):
    def __init__(self, uri, content_type, token, refresh):
        self.content_type = content_type
        self.refresh = refresh
        self.token = token
        self.uri = uri
        self.buf = BytesIO()

    def __enter__(self):
        return self

    def __exit__(self, t, value, traceback):
        self.close()

    def _put(self):
        return requests.put(
                self.uri,
                data=self.buf,
                headers={
                    'X-Auth-Token': self.token,
                    'Content-Type': self.content_type
                })

    def close(self):
        if self.buf.tell() == 0:
            # no bytes have been written to the stream
            return

        self.buf.seek(0)
        resp = self._put()
        if resp.status_code == httplib.UNAUTHORIZED:
            self.token = self.refresh()
            self._put()

    def write(self, data):
        self.buf.write(data)


class ObjectStoreDatabase(Database):
    def __init__(
            self,
            container_name,
            username,
            api_key,
            region,
            cdn_ttl_seconds=15 * 60,
            key_builder=None):

        super(ObjectStoreDatabase, self).__init__(key_builder=key_builder)
        self.cdn_ttl_seconds = cdn_ttl_seconds
        self.region = region
        self.api_key = api_key
        self.username = username
        self.container_name = container_name
        self.token = None
        self.tenant = None
        self.endpoint = None
        self.cdn_endpoint = None
        self.cdn_uri = None

        self._get_token()
        self._create_container()

    def _get_token(self):
        resp = requests.post(
                'https://identity.api.rackspacecloud.com/v2.0/tokens',
                data=json.dumps({
                    'auth': {
                        'RAX-KSKEY:apiKeyCredentials': {
                            'username': self.username,
                            'apiKey': self.api_key
                        }
                    }
                }),
                headers={'Content-Type': 'application/json'})
        resp.raise_for_status()

        data = resp.json()
        self.token = data['access']['token']['id']
        self.tenant = data['access']['token']['tenant']['id']

        for service in data['access']['serviceCatalog']:
            if service['name'] == 'cloudFilesCDN':
                self.cdn_endpoint = filter(
                        lambda x: x['region'] == self.region,
                        service['endpoints'])[0]['publicURL']
            if service['name'] == 'cloudFiles':
                self.endpoint = filter(
                        lambda x: x['region'] == self.region,
                        service['endpoints'])[0]['publicURL']
            if self.endpoint and self.cdn_endpoint:
                break

        return self.token

    def _create_container(self):
        # PUT the container
        resp = requests.put(
                '{endpoint}/{container_name}'.format(**self.__dict__),
                headers={'X-Auth-Token': self.token})
        resp.raise_for_status()

        # Enable CDN
        resp2 = requests.put(
                '{cdn_endpoint}/{container_name}'.format(**self.__dict__),
                headers={
                    'X-Auth-Token': self.token,
                    'X-CDN-Enabled': 'True',
                    'X-TTL': str(self.cdn_ttl_seconds)
                })
        resp2.raise_for_status()

        self.cdn_uri = resp2.headers['x-cdn-uri']

    def _uri(self, key):
        key = urllib.quote(key, safe='')
        return '{endpoint}/{container_name}/{key}'.format(
                endpoint=self.endpoint,
                container_name=self.container_name,
                key=key)

    def _cdn_uri(self, key):
        key = urllib.quote(key, safe='')
        return '{cdn_uri}/{key}'.format(
                cdn_uri=self.cdn_uri,
                key=key)

    def write_stream(self, key, content_type):
        return WriteStream(
                self._uri(key), content_type, self.token, self._get_token)

    def read_stream(self, key):
        uri = self._cdn_uri(key)
        resp = requests.get(uri, stream=True)

        if resp.status_code == httplib.NOT_FOUND:
            raise KeyError(key)

        return BytesIO(resp.raw.read())

    def size(self, key):
        uri = self._cdn_uri(key)
        resp = requests.head(uri)
        if resp.status_code != httplib.OK:
            raise KeyError(key)
        return int(resp.headers['Content-Length'])

    def _all_keys(self):
        container_uri = '{endpoint}/{container_name}'.format(**self.__dict__)
        resp = requests.get(
                container_uri,
                headers={'X-Auth-Token': self.token})
        for whole_key in resp.content.splitlines():
            yield whole_key

    def iter_ids(self):
        seen = set()
        for whole_key in self._all_keys():
            _id, key, version = self.key_builder.decompose(whole_key)
            if _id not in seen:
                yield urllib.unquote(_id)
                seen.add(_id)

    def __contains__(self, key):
        uri = self._uri(key)
        resp = requests.head(uri, headers={'X-Auth-Token': self.token})
        return resp.status_code == httplib.OK

    def __delitem__(self, key):
        uri = self._uri(key)
        resp = requests.delete(
                uri,
                headers={'X-Auth-Token': self.token})
        if resp.status_code != httplib.NO_CONTENT:
            raise KeyError(key)

    def __del__(self):
        container_uri = '{endpoint}/{container_name}'.format(**self.__dict__)
        for whole_key in self._all_keys():
            try:
                del self[whole_key]
            except KeyError:
                pass

        resp = requests.delete(
                container_uri,
                headers={'X-Auth-Token': self.token})

        resp.raise_for_status()
