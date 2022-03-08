[![AppVeyor](https://img.shields.io/docker/cloud/build/txscience/fuse-agent?style=plastic)](https://hub.docker.com/repository/docker/txscience/fuse-agent/builds)

# fuse-agent

Configure this repo to write apps against orchestrated, generic data providers and tools.

FUSE stands for "[FAIR](https://www.go-fair.org/)", Usable, Sustainable, and Extensible.

FUSE appliances can be run as a stand-alone appliance (see `up.sh` below) or as a plugin to a FUSE deployment (e.g., [fuse-immcellfie](http://github.com/RENCI/fuse-immcellfie)). FUSE appliances come in 3 flavors:
* provider: provides a common data access protocol to a digital object provider
* mapper: maps the data from a particular data provider type into a common data model with consistent syntax and semantics
* tool: analyzes data from a mapper, providing results and a specification that describes the data types and how to display them.

This "agent" appliance behaves most like a provider, orchestrating services from appliances of types listed above.

## prerequisites:
* python 3.8 or higher
* Docker 20.10 or higher
* docker-compose v1.28 a
### for testing the install:
* perl 5.16.3 or higher
* cpan
* cpanm
* jq
Be sure PERL5LIB is set to where your libraries are installed, e.g. something like the following may work:
```
export PERL5LIB=/home/${USER}/perl5/lib/perl5
```
Also be sure locale is appropriately set on the linux machine, if not you may need something like the following in your .bashrc or .bash_profile
```
# Setting for the new UTF-8 terminal support in Lion
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```



### tips for updating docker-compose on Centos:

```
sudo yum install jq
VERSION=$(curl --silent https://api.github.com/repos/docker/compose/releases/latest | jq .name -r)
sudo mv /usr/local/bin/docker-compose /usr/local/bin/docker-compose.old-version
DESTINATION=/usr/local/bin/docker-compose
sudo curl -L https://github.com/docker/compose/releases/download/${VERSION}/docker-compose-$(uname -s)-$(uname -m) -o $DESTINATION
sudo chmod 755 $DESTINATION
```

## configuration

1. Get this repository:
`git clone --recursive http://github.com/RENCI/fuse-agent

2. Copy `sample.env` to `.env` and edit to suit your provider:
* __HOST_PORT__ pick a unique port to avoid appliances colliding with each other


3. Configure `config\config.json`: We use configuration by convention, so be sure the services that are 'providers' start with 'fuse-provider-' in the config file, and tools start with 'fuse-tool-'. Changes to the config file take place immediately, no need to restart.

## start
```
./up.sh
```

## validate installation

Start container prior to validations:
```
./up.sh
```
Simple test from command line

```
curl -X 'GET' 'http://localhost:${API_PORT}/config' -H 'accept: application/json' |jq -r 2> /dev/null
```
Install test dependencies:
```
cpan App::cpanminus
# restart shell
cpanm --installdeps .

```
Run tests:
```
prove
```
More specific, detailed test results:
```
prove -v t/test.t :: --verbose
```

## stop
```
./down.sh
```
