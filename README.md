DMWM client
-----------
Hacks and scripts to use in CMS DMWM dev/ops.


## Release installation
```
pip3 install dmwmclient
```

## Usage
Three ways to interact with this package are envisaged:
 - python script
 - `dmwm` command-line interface
 - jupyter notebook

### CLI example
Here we use the `dmwm shell` command to launch an IPython shell with the various API clients instantiated.
We make a few simple requests to datasvc and reqmgr.
```
$ dmwm shell
Python 3.7.6 (default, Dec 30 2019, 19:38:36)
Type 'copyright', 'credits' or 'license' for more information
IPython 7.10.2 -- An enhanced Interactive Python. Type '?' for help.


Local variables: client (<dmwmclient.Client object at 0x111172810>)

In [1]: df = await client.datasvc.blockreplicas(dataset='/EGamma/Run2018D-22Jan2019-v2/AOD')

In [2]: df
Out[2]:
      replica.bytes  replica.files       replica.node replica.time_create  ... files                                               name        id is_open
0      859056433305            135    T1_US_FNAL_Disk 2019-03-16 15:12:37  ...   135  /EGamma/Run2018D-22Jan2019-v2/AOD#57b25659-3e2...  10992784       n
1      859056433305            135    T1_RU_JINR_Disk 2019-03-02 12:54:55  ...   135  /EGamma/Run2018D-22Jan2019-v2/AOD#57b25659-3e2...  10992784       n
2      859056433305            135     T1_DE_KIT_Disk 2019-03-09 12:05:02  ...   135  /EGamma/Run2018D-22Jan2019-v2/AOD#57b25659-3e2...  10992784       n
3      859056433305            135     T1_US_FNAL_MSS 2019-03-29 23:29:13  ...   135  /EGamma/Run2018D-22Jan2019-v2/AOD#57b25659-3e2...  10992784       n
4      859056433305            135  T1_US_FNAL_Buffer 2019-04-01 10:02:04  ...   135  /EGamma/Run2018D-22Jan2019-v2/AOD#57b25659-3e2...  10992784       n
...             ...            ...                ...                 ...  ...   ...                                                ...       ...     ...
1495  1249881194222            201    T1_RU_JINR_Disk 2019-03-12 01:04:44  ...   201  /EGamma/Run2018D-22Jan2019-v2/AOD#f5d3628e-1c7...  11021144       n
1496  1249881194222            201    T1_US_FNAL_Disk 2019-03-16 15:12:37  ...   201  /EGamma/Run2018D-22Jan2019-v2/AOD#f5d3628e-1c7...  11021144       n
1497  1249881194222            201     T1_DE_KIT_Disk 2019-03-08 00:38:51  ...   201  /EGamma/Run2018D-22Jan2019-v2/AOD#f5d3628e-1c7...  11021144       n
1498  1249881194222            201     T1_US_FNAL_MSS 2019-03-29 23:29:13  ...   201  /EGamma/Run2018D-22Jan2019-v2/AOD#f5d3628e-1c7...  11021144       n
1499  1249881194222            201  T1_US_FNAL_Buffer 2019-04-01 06:08:03  ...   201  /EGamma/Run2018D-22Jan2019-v2/AOD#f5d3628e-1c7...  11021144       n

[1500 rows x 16 columns]

In [3]: df.groupby('replica.node').sum()
Out[3]:
                     replica.bytes  replica.files  replica.node_id
replica.node
T1_DE_KIT_Disk     215484127440685          33550           546300
T1_RU_JINR_Disk    215484127440685          33550           523500
T1_US_FNAL_Buffer  215484127440685          33550             2700
T1_US_FNAL_Disk    215484127440685          33550           534300
T1_US_FNAL_MSS     215484127440685          33550             3000

In [4]: df = await client.reqmgr.transitions(outputdataset='/EGamma/Run2018D-22Jan2019-v2/AOD')

In [5]: df[df['current']]
Out[5]:
                                          requestname  current               mask  ...           Status                                                 DN          UpdateTime
7   vlimant_ACDC0_Run2018D-v1-EGamma-22Jan2019_102...     True  RequestTransition  ...       closed-out  /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=vl... 2019-03-27 13:14:10
17  prebello_Run2018D-v1-EGamma-22Jan2019_1025p1_1...     True  RequestTransition  ...  normal-archived  /DC=ch/DC=cern/OU=computers/CN=dmwm/cmsweb.cer... 2019-04-08 14:34:20

[2 rows x 7 columns]
```

## Developer installation:
```
git clone git@github.com:nsmith-/dmwmclient.git
cd dmwmclient
pip3 install -e .[dev]
# do some development
black dmwmclient
flake8 dmwmclient
```
Please run the `flake8` linter and `black` code formatter before committing.
