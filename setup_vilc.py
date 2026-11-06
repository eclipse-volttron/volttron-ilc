"""Install the standalone ``vilc`` command on the Monolithic platform.

Monolithic's ``vctl install`` builds the agent from ``setup.py`` and unpacks it into the agent's own directory, so no
console script reaches the virtual environment. Run this file in the platform's active virtual environment to add
``vilc`` to its ``bin``::

    python setup_vilc.py develop

On the modular platform do not use this file: the same commands are available as ``vctl ilc`` through the vctl
plugin that installs with the agent.
"""
from setuptools import find_namespace_packages, setup

setup(
    name="vilc",
    packages=find_namespace_packages("./src"),
    package_dir={'': 'src'},
    entry_points={"console_scripts": ["vilc = volttron.plugins.vctl.ilc.parser:main"]},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Home Automation",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.10",
    ],
)
