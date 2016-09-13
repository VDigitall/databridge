from setuptools import setup

install_requires = [
    'gevent',
    'requests',
    'CouchDB'
]


setup(
    name='databridge',
    version='0.0.1',
    packages=[
        'databridge',
    ],
    install_requires=install_requires,
)
