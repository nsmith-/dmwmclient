import asyncio
import os
import datetime
import json
import re
import logging
import httpx
import pandas
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
    def account(self, account):
        self._account = account
        self._token_expiration = None
        if self._account is not None:
            self._headers = {"X-Rucio-Account": self._account}

    async def check_token(self, validate=False):
        async with self._token_lock:
            if self._token_expiration is None or self._token_expiration < datetime.datetime.utcnow() + datetime.timedelta(
                minutes=5
            ):
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
            elif validate:
                token_req = self.client.build_request(
                    method="GET",
                    url=self.auth_host.join("auth/validate"),
                    headers=self._headers,
                )
                response = await self.client.send(token_req)
                m = self._lifetime.match(response.text)
                if not m:
                    raise RuntimeError(
                        "Bad response from auth endpoint:\n" + response.text
                    )
                ts = m.groups()[0]
                self._token_expiration = datetime.datetime(*map(int, ts.split(",")))

    async def jsonmethod(
        self, method, path, params=None, jsondata=None, timeout=None, retries=1
    ):
        await self.check_token()
        request = self.client.build_request(
            method=method,
            url=self.host.join(path),
            params=params,
            json=jsondata,
            headers=self._headers,
        )
        result = await self.client.send(request, timeout=timeout, retries=retries)
        if result.status_code != 200:
            raise IOError(
                f"Failed to execute request {request}, result: ({result.status_code}) {result.text}"
            )
        try:
            items = filter(len, result.text.split("\n"))
            return list(map(json.loads, items))
        except json.JSONDecodeError:
            logger.debug(f"Result content:\n{result.text}")
            raise IOError(f"Failed to decode json for request {request}")

    async def getjson(self, path, params=None, timeout=None, retries=1):
        return await self.jsonmethod(
            "GET", path, params=params, timeout=timeout, retries=retries
        )

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

    async def list_rules(self, **filters):
        """List rules by filters

        Possible filters: anything in the ReplicationRule model it seems
        """
        return await self.getjson("rules/", params=filters)

    async def examine_rule(self, rule_id):
        """Get rule analysis

        Note: this can take an extended time to return
        """
        return await self.getjson(f"rules/{rule_id}/analysis", timeout=60)

    async def list_did_rules(self, scope, name):
        """Shows the rules tha apply to a specific did.
        Parameters
        ----------
        name                  name of container, dataset or file.
        scope                 scope = 'cms'.
        """
        scope = quote(scope, safe="")
        name = quote(name, safe="")
        method = "/".join(["dids", scope, name, "rules"])
        data = await self.getjson(method)
        out = []
        for dic in data:
            out.append(
                {
                    "id": dic["id"],
                    "locks_ok_cnt": dic["locks_ok_cnt"],
                    "did_type": dic["did_type"],
                    "weight": dic["weight"],
                    "purge_replicas": dic["purge_replicas"],
                    "rse_expression": rse,
                    "updated_at": dic["updated_at"],
                    "activity": dic["activity"],
                    "child_rule_id": dic["child_rule_id"],
                    "locks_stuck_cnt": dic["locks_stuck_cnt"],
                    "locks_replicating_cnt": dic["locks_replicating_cnt"],
                    "copies": dic["copies"],
                    "comments": dic["comments"],
                    "split_container": dic["split_container"],
                    "state": dic["state"],
                    "scope": dic["scope"],
                    "subscription_id": dic["subscription_id"],
                    "stuck_at": dic["stuck_at"],
                    "expires_at": dic["expires_at"],
                    "account": dic["account"],
                    "locked": dic["locked"],
                    "name": dic["name"],
                    "grouping": dic["grouping"],
                }
            )
        df = pandas.json_normalize(out)
        return df

    async def delete_rule(self, rule_id, purge_replicas=None, immediate=False):
        await self.check_token()
        if immediate:
            data = {"options": {"lifetime": 0}}
            request = self.client.build_request(
                method="PUT",
                url=self.host.join("rules/" + rule_id),
                json=data,
                headers=self._headers,
            )
        else:
            data = {"purge_replicas": purge_replicas}
            request = self.client.build_request(
                method="DELETE",
                url=self.host.join("rules/" + rule_id),
                json=data,
                headers=self._headers,
            )
        result = await self.client.send(request)
        if result.status_code not in [200, 404]:
            raise ValueError(
                f"Received {result.status_code} status while deleting rule"
            )

    async def list_content(self, scope, name):
        scope = quote(scope, safe="")
        name = quote(name, safe="")
        method = "/".join(["dids", scope, name, "dids"])
        data = await self.getjson(method)
        out = []
        for key in data:
            out.append(
                {
                    "adler_32": key["adler32"],
                    "lfn": key["name"],
                    "bytes": key["bytes"],
                    "scope": key["scope"],
                    "type": key["type"],
                }
            )
        df = pandas.json_normalize(out)
        return df

    async def list_replicas(self, scope, name):
        """Shows the file replicas.
        Parameters
        ----------
        name                  name of container, dataset or file.
        scope                 scope = 'cms'.
        """
        scope = quote(scope, safe="")
        name = quote(name, safe="")
        method = "/".join(["replicas", scope, name])
        data = await self.getjson(method)
        out = []
        for instance in data:
            for _pfn in instance["pfns"].keys():
                out.append(
                    {
                        "adler_32": instance["adler32"],
                        "lfn": instance["name"],
                        "bytes": instance["bytes"],
                        "pfn": _pfn,
                        "replica": instance["pfns"][_pfn]["rse"],
                    }
                )
        df = pandas.json_normalize(out)
        return df

    async def list_dataset_replicas(self, scope, name):
        """Shows replicas of datasets (former block in phedex context).
        Parameters
        ----------
        name                name of the dataset (block in phedex context). This function returns an 
                            empty dataframe if a name of a container (former dataset in phedex context)
                            is passed as a parameter instead of the name of a dataset.
        scope               scope = 'cms'.
        """
        scope = quote(scope, safe="")
        name = quote(name, safe="")
        method = "/".join(["replicas", scope, name, "datasets"])
        data = await self.getjson(method)
        out = []
        for element in data:
            out.append(
                {
                    "accessed_at": element["accessed_at"],
                    "dataset_name": element["name"],
                    "rse": element["rse"],
                    "created_at": element["created_at"],
                    "Total_bytes": element["bytes"],
                    "Bytes_at_rse": element["available_bytes"],
                    "state": element["state"],
                    "updated_at": element["updated_at"],
                    "Total_files": element["length"],
                    "files_at_rse": element["available_length"],
                    "rse_id": element["rse_id"],
                }
            )
        df = pandas.json_normalize(out)
        return df
    async def set_local_account_limit(self, account, rse, nbytes):
        await self.check_token()
        method = "/".join(["accountlimits", "local", account, rse])
        data = {"bytes": int(nbytes)}
        request = self.client.build_request(
            method="POST", url=self.host.join(method), json=data, headers=self._headers,
        )
        result = await self.client.send(request)
        if result.status_code == 201:
            return result.text
        raise ValueError(f"Received {result.status_code} status while creating rule")
