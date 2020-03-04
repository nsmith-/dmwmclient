DMWM client
-----------
Hacks and scripts to use in CMS DMWM dev/ops.


## Release installation
```
pip3 install https://github.com/nsmith-/dmwmclient/archive/master.zip
```

## Usage
Three ways to interact with this package are envisaged:
 - python script
 - `dmwm` command-line interface
 - jupyter notebook

## Developer installation:
```
git clone git@github.com:nsmith-/dmwmclient.git
cd dmwmclient
pip3 install -e .[dev]
# do some development
python -m black dmwmclient
flake8 dmwmclient
```
Please run the `flake8` linter and `black` code formatter before committing.
