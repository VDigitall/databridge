from setuptools import setup

install_requires = [
    'gevent',
    'requests',
#    'couchdbreq',
    'grequests',
    'PyYaml',
    'ujson'
]


setup(
    name='databridge',
    version='0.0.1',
    packages=[
        'databridge',
    ],
    entry_points={
        'console_scripts': [
            'bridge = databridge.run:run'
        ]
    },
    install_requires=install_requires,
)
