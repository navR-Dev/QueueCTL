from setuptools import setup

setup(
    name="queuectl",
    version="1.0",
    py_modules=["cli", "job", "storage", "worker", "dlq"],
    install_requires=["click"],
    entry_points={
        "console_scripts": [
            "queuectl=cli:cli"
        ]
    },
)
