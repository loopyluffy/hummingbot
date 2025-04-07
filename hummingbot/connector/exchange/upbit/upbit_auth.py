import hashlib

# import hmac
# import json
import uuid

# from collections import OrderedDict
# from typing import Any, Dict
from urllib.parse import unquote, urlencode

import jwt

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest  # , RESTMethod


class UpbitAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update({"Authorization": self.generate_payload(request)})
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # payload = {
        #     'access_key': self.api_key,
        #     'nonce': str(uuid.uuid4()),
        # }

        # jwt_token = jwt.encode(payload, self.secret_key)
        # authorization_token = 'Bearer {}'.format(jwt_token)
        # headers = {"Authorization": authorization_token}
        # request.headers = headers
        request.headers = self.get_auth_headers()

        return request  # pass-through

    def get_auth_headers(self):
        payload = {
            'access_key': self.api_key,
            'nonce': str(uuid.uuid4()),
        }

        jwt_token = jwt.encode(payload, self.secret_key)
        authorization_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorization_token}

        return headers

    def generate_payload(self, request):
        params = request.params
        data = request.data

        payload = {
            'access_key': self.api_key,
            'nonce': str(uuid.uuid4())
        }
        # if data:
        if isinstance(data, dict):
            params.update(data)
            # else:
            #     data_dict = json.loads(data)
            #     params.update(data_dict)
            #     # params.update(json.loads(data))
        if params:
            query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")
            m = hashlib.sha512()
            m.update(query_string)
            query_hash = m.hexdigest()

            payload["query_hash"] = query_hash
            payload["query_hash_alg"] = 'SHA512'

        jwt_token = jwt.encode(payload, self.secret_key)
        authorize_token = f"Bearer {jwt_token}"

        return authorize_token
