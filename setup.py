from setuptools import setup, find_packages

setup(
    name='es_utils',
    version='0.1',
    packages=find_packages(),  # Required
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        es-reshard=module.reshard:cli
    ''',
)
