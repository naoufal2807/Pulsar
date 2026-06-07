# Build pulsar python package
from setuptools import setup, find_packages

# Setup stage
setup(
    name="pulsar", # package name on PyPI
    version ="0.1.0" , # Semantic versioning
    description= "...", # One-liner
    author="NAOUFAL SAADI",
    packages=find_packages(), # Auto-find all packages (folders with __init__.py)
    python_requires=">=3.10",  


    # Install requires
    install_requires=[
        "polars>=0.19.0", # At least version 0.19.0
        "typer>=0.9.0", # For CLI
        "pyyaml>=6.0"  # For YAML input parsing
    ],


    # extra requirements

    extras_requires={
        "dev": [
        "pytest>=7.0.0", # For testing
        "pytest-cov>=4.0.0" # For coverage reports
        ],
    },

    entry_points={
        "console_scripts":[
        "pulsar=pulsar.cli:app", # `pulsar` command -> pulsar.cli.app
        ],
    },

)