import asyncio
import os
import datetime
import json
import re
import logging
import httpx
from urllib.parse import quote


logger = logging.getLogger(__name__)


class Rucio:
    _lifetime = re.compile(r".*datetime\.datetime\(([0-9 ,]*)\)")

    def __init__(self, client, account=None, host=None, auth_host=None):
        self.host = httpx.URL("http://cms-rucio.cern.ch" if host is None else host)
        self.auth_host = httpx.URL(
            "https://cms-rucio-auth.cern.ch" if auth_host is None else auth_host
        )
        self.client = client
        self._token_lock = asyncio.Lock()
        self._token_expiration = None
        self._headers = {}
        self._account = os.getenv("RUCIO_ACCOUNT", account)
        if self._account is not None:
            self._headers = {"X-Rucio-Account": self._account}

    @property
    def account(self):
        return self._account

    @account.setter
    def set_account(self, account):
        self._account = account
        self._token_expiration = None
        if self._account is not None:
            self._headers = {"X-Rucio-Account": self._account}

    async def check_token(self, validate=False):
        async with self._token_lock:
            if self._token_expiration is None:
                token_req = self.client.build_request(
                    method="GET",
                    url=self.auth_host.join("auth/x509_proxy"),
                    headers=self._headers,
                )
                response = await self.client.send(token_req)
                logger.debug(f"Auth response headers: {response.headers}")
                self._headers = {
                    "X-Rucio-Auth-Token": response.headers["x-rucio-auth-token"]
                }
                self._token_expiration = datetime.datetime.strptime(
                    response.headers["X-Rucio-Auth-Token-Expires"],
                    "%a, %d %b %Y %H:%M:%S %Z",
                )
            elif validate or self._token_expiration < datetime.datetime.utcnow() + datetime.timedelta(
                minutes=5
            ):
                token_req = self.client.build_request(
                    method="GET",
                    url=self.auth_host.join("auth/validate"),
                    headers=self._headers,
                )
                response = await self.client.send(token_req)
                m = self._lifetime.match(response.text)
                if not m:
                    raise RuntimeError("Bad response from auth endpoint")
                ts = m.groups()[0]
                self._token_expiration = datetime.datetime(*map(int, ts.split(",")))

    async def getjson(self, method, params=None, timeout=None, retries=1):
        await self.check_token()
        request = self.client.build_request(
            method="GET",
            url=self.host.join(method),
            params=params,
            headers=self._headers,
        )
        result = await self.client.send(request, timeout=timeout, retries=retries)
        if result.status_code != 200:
            raise IOError("Failed to execute request {request}")
        try:
            items = filter(len, result.text.split("\n"))
            return list(map(json.loads, items))
        except json.JSONDecodeError:
            logger.debug(f"Result content:\n{result.text}")
            raise IOError(f"Failed to decode json for request {request}")

    async def whoami(self):
        return await self.getjson("accounts/whoami")

    async def add_rule(self, rule):
        """
        rule is a json-compatible object following definition at:
        https://rucio.readthedocs.io/en/latest/restapi/rule.html#post--rule-
        """
        rule.setdefault("grouping", "ALL")
        rule.setdefault("account", self._account)
        rule.setdefault("locked", False)
        rule.setdefault("notify", "N")
        rule.setdefault("purge_replicas", False)
        rule.setdefault("ignore_availability", False)
        rule.setdefault("ask_approval", False)
        rule.setdefault("asynchronous", False)
        rule.setdefault("priority", 3)
        await self.check_token()
        request = self.client.build_request(
            method="POST",
            url=self.host.join("rules/"),
            json=rule,
            headers=self._headers,
        )
        result = await self.client.send(request)
        if result.status_code in {201, 409}:
            return result.json()
        raise ValueError(f"Received {result.status_code} status while creating rule")

    async def list_did_rules(self, scope, name):
        scope = quote(scope, safe="")
        name = quote(name, safe="")
        method = "/".join(["dids", scope, name, "rules"])
        return await self.getjson(method)

    async def delete_rule(self, rule_id, purge_replicas=None):
        await self.check_token()
        data = {"purge_replicas": purge_replicas}
        request = self.client.build_request(
            method="DELETE",
            url=self.host.join("rules/" + rule_id),
            json=data,
            headers=self._headers,
        )
        result = await self.client.send(request)
        return result
        if result.status_code != 200:
            raise ValueError(
                f"Received {result.status_code} status while deleting rule"
            )

    async def list_content(self, scope, name):
        scope = quote(scope, safe="")
        name = quote(name, safe="")
        method = "/".join(["dids", scope, name, "dids"])
        return await self.getjson(method)

    async def list_replicas(self, scope, name):
        scope = quote(scope, safe="")
        name = quote(name, safe="")
        method = "/".join(["replicas", scope, name])
        return await self.getjson(method)

    async def list_dataset_replicas(self, scope, name):
        scope = quote(scope, safe="")
        name = quote(name, safe="")
        method = "/".join(["replicas", scope, name, "datasets"])
        return await self.getjson(method)
