[![AppVeyor](https://img.shields.io/docker/cloud/build/txscience/fuse-appliance-template?style=plastic)](https://hub.docker.com/repository/docker/txscience/fuse-appliance-template/builds)

# fuse-appliance-template

Clone this repo to create a new FUSE-style appliance.

FUSE stands for "[FAIR](https://www.go-fair.org/)", Usable, Sustainable, and Extensible.

FUSE appliances can be run as a stand-alone appliance (see `up.sh` below) or as a plugin to a FUSE deployment (e.g., [fuse-immcellfie](http://github.com/RENCI/fuse-immcellfie)). FUSE appliances come in 3 flavors:
* provider: provides a common data access protocol to a digital object provider
* mapper: maps the data from a particular data provider type into a common data model with consistent syntax and semantics
* tool: analyzes data from a mapper, providing results and a specification that describes the data types and how to display them.

## prerequisites:
* python 3.8 or higher
* Docker 20.10 or higher
* docker-compose v1.28 a
* perl 5.16.3 or higher (for testing the install)
* cpan
* jq

Tips for updating docker-compose on Centos:

```
sudo yum install jq
VERSION=$(curl --silent https://api.github.com/repos/docker/compose/releases/latest | jq .name -r)
sudo mv /usr/local/bin/docker-compose /usr/local/bin/docker-compose.old-version
DESTINATION=/usr/local/bin/docker-compose
sudo curl -L https://github.com/docker/compose/releases/download/${VERSION}/docker-compose-$(uname -s)-$(uname -m) -o $DESTINATION
sudo chmod 755 $DESTINATION
```

## use this template:

Note, an appliance must specify a pluginType, so for the purpose of this demonstration we use type 'p' (for digital object 'Provider')
* To add a new repository to the RENCI organization, [click here](https://github.com/organizations/RENCI/repositories/new) and select this repo for the template, otherwise new repo will be added to your username.
* Get your new repo using the 'recursive' tag (see below)
```
git clone --recursive http://github.com/RENCI/<your-repo-name>
```
* Make sure the test passes (`./up.sh; prove t/test.t` - check t/test.t on how to install the test harness dependencies)
* Edit this README.md file and replace all occurrences of `fuse-appliance-template` with your repo's name
* Update the source files appropriately:
 - [ ] **config.json**: describe your appliances pluginType ["p":"Provider", "m":"Mapper", "t":"Tool"], required parameters, and supported/required objectVariables
 - [ ] **docker-compose.yml**: replace `fuse-appliance-template` with your repo's name and customize accordingly
 - [ ] **requirements.txt**: add your *version-locked* library requirements to the list
 - [ ] **sample.env**: add any required environmental variables, don't forget to also document them in this readme
 - [ ] **main.py**: 
   - [ ] Search for all occurrences of `fuse-appliance-template` and replace
   - [ ] Define and add endpoints for your appliance
   - [ ] Create functions in ./lib to support you endpoints, with unit tests, adding the unit tests to github actions (examples coming soon!)
 - [ ] **write and run tests - look at t/test.t for examples
 - [ ] contact the dockerhub/txscience organization administrator (email:txscience@lists.renci.org) to add a dockerhub repo for your container, if needed
* remove this section from the README.md
* checkin your mods: 
```
git status # make sure everything looks OK
git commit -a -m 'Initial customization'
git push
```

## configuration

1. Get this repository:
`git clone --recursive http://github.com/RENCI/fuse-appliance-template

2. Copy `sample.env` to `.env` and edit to suit your provider:
* __API_PORT__ pick a unique port to avoid appliances colliding with each other

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
