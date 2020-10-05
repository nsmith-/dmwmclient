from .version import __version__
from .restclient import RESTClient
from .datasvc import DataSvc
from .unified import Unified
from .dbs import DBS
from .reqmgr import ReqMgr
from .dynamo import Dynamo
from .mcm import McM
from .msmgr import MSMgr
from .rucio import Rucio


class Client(RESTClient):
    def __init__(self, usercert=None, certdir=None):
        super().__init__(usercert, certdir)
        self.datasvc = DataSvc(self)
        self.unified = Unified(self)
        self.dbs = DBS(self)
        self.reqmgr = ReqMgr(self)
        self.dynamo = Dynamo(self)
        self.mcm = McM(self)
        self.msmgr = MSMgr(self)
        self.rucio = Rucio(self)


__all__ = [
    "__version__",
    "RESTClient",
    "DataSvc",
    "Unified",
    "DBS",
    "ReqMgr",
    "Dynamo",
    "Client",
    "MSMgr",
]
