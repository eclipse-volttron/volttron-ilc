# Intelligent Load Control (ILC) Agent

![Eclipse VOLTTRON 10.0.5rc0](https://img.shields.io/badge/Eclipse%20VOLTTRON-10.0.5rc0-red.svg)
![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
[![pypi version](https://img.shields.io/pypi/v/volttron-ilc.svg)](https://pypi.org/project/volttron-ilc/)

Main branch tests:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; [![Main Branch Passing?](https://github.com/eclipse-volttron/volttron-ilc/actions/workflows/run-tests.yml/badge.svg?branch=main)](https://github.com/eclipse-volttron/volttron-ilc/actions/workflows/run-tests.yml)

Develop branch tests:&nbsp;&nbsp; [![Develop Branch Passing?](https://github.com/eclipse-volttron/volttron-ilc/actions/workflows/run-tests.yml/badge.svg?branch=develop)](https://github.com/eclipse-volttron/volttron-ilc/actions/workflows/run-tests.yml)


ILC supports traditional demand response as well as transactive energy
services. ILC manages controllable loads while also mitigating
service-level excursions (e.g., occupant comfort, minimizing equipment
ON/OFF cycling) by dynamically prioritizing available loads for curtailment
using both quantitative (deviation of zone conditions from set point) and
qualitative rules (type of zone).

## Requirements

* python >= 3.10
* volttron >= 10.0 
* sympy >= 1.12
* transitions >= 0.9.0

## Documentation

More detailed documentation can be found on
[ReadTheDocs](https://eclipse-volttron.readthedocs.io/en/latest/external-docs/volttron-ilc/index.html). The RST source
of the documentation for this agent is located in the "docs" directory of this repository.

## ILC Agent Configuration

The  ILC agent, requires four configuration files per device cluster (i.e., homogenous set of devices).  These
files should be loaded into the agent's config store via vctl config command line interface.  Samples of these files
may be found in the sample_configs directory of this repository:
 1. config - ILC (main) configuration.
 2. control_config - Contains information related to the control of device cluster. 
 3. criteria_config - Contains information related to the use of real time data to prioritize devices within
    cluster for load management. 
 4. pairwise_criteria.json - Contains information related to the relative importance of each criteria for a device cluster.
    
Full documentation of ILC configurations can be found on
[ReadTheDocs](https://eclipse-volttron.readthedocs.io/en/latest/external-docs/volttron-ilc/index.html).
A web-based configuration tool has been developed to simplify creation of the configuration files for ILC.
The web tool can be accessed [here](https://ilc-configuration-tool.web.app/).

Instructions for the configuration web-tool can be found [here](https://userguide-ilc.readthedocs.io/en/latest/).

## Installation

Before installing, VOLTTRON should be installed and running.  Its virtual environment should be active.
Information on how to install of the VOLTTRON platform can be found
[here](https://github.com/eclipse-volttron/volttron-core).

Install and start the Intelligent Load Control Agent.

```shell
vctl config store ilc.agent config <path of config file>
vctl config store ilc.agent control_config <path to device_control_config>
vctl config store ilc.agent criteria_config <path to device_criteria_config>
vctl config store ilc.agent pairwise_criteria.json <path to pairwise_criteria_config>
vctl install volttron-ilc --vip-identity ilc.agent --tag ilc --start
```

View the status of the installed agent

```shell
vctl status
```

### Command-line tools

Installing the agent on the modular platform also installs a `vctl` plugin. `vctl ilc target` publishes a demand
target to the agent:

```shell
vctl ilc target 150 --start 2026-11-05T14:00:00 --end 2026-11-05T16:00:00
```

The Monolithic platform's `vctl` has no plugin mechanism, so the same commands are provided there by a standalone
`vilc` script (`vilc ilc target ...`). `vctl install` does not place it on the path; install it once from this
directory with the platform's virtual environment active:

```shell
python setup_vilc.py develop
```

Both share `vctl`'s control connection identity, so do not run them while another `vctl` command is in progress.

## Development

Please see the following for contributing guidelines [contributing](https://github.com/eclipse-volttron/volttron-core/blob/develop/CONTRIBUTING.md).

Please see the following helpful guide about [developing modular VOLTTRON agents](https://github.com/eclipse-volttron/volttron-core/blob/develop/DEVELOPING_ON_MODULAR.md)
