from .version import __version__
from .restclient import RESTClient
from .datasvc import DataSvc
from .unified import Unified
from .dbs import DBS
from .reqmgr import ReqMgr
from .dynamo import Dynamo
from .mcm import McM


class Client:
    def __init__(self, usercert=None):
        self.baseclient = RESTClient(usercert=usercert)
        self.datasvc = DataSvc(self.baseclient)
        self.unified = Unified(self.baseclient)
        self.dbs = DBS(self.baseclient)
        self.reqmgr = ReqMgr(self.baseclient)
        self.dynamo = Dynamo(self.baseclient)
        self.mcm = McM(self.baseclient)


__all__ = [
    "__version__",
    "RESTClient",
    "DataSvc",
    "Unified",
    "DBS",
    "ReqMgr",
    "Dynamo",
    "Client",
]
