from setuptools import setup

setup(
    author="azcoigreach",
    author_email="azcoigreach@gmail.com",
    name = 'MongoDB Data Tools',
    version = '0.4.0',
    packages=['data_tools','data_tools.commands','data_tools.configs'],
    include_package_data=True,
    install_requires = [
        'click',
        'colorama >= 0.3.9',
        'coloredlogs',
        'pymongo',
        'pyfiglet',
        'dash',
        'dash-renderer',
        'dash-html-components',
        'dash-core-components',
        'plotly',
    ],
    entry_points = '''
        [console_scripts]
        data_tools=data_tools.cli:cli
    ''',
)