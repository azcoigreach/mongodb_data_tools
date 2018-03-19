from setuptools import setup

setup(
    author="azcoigreach",
    author_email="azcoigreach@gmail.com",
    name = 'MongoDB Data Tools',
    version = '0.3.2',
    packages=['data_tools','data_tools.commands','data_tools.configs'],
    include_package_data=True,
    install_requires = [
        'click',
        'colorama',
        'coloredlogs',
        'pymongo',
        'pyfiglet'        
    ],
    entry_points = '''
        [console_scripts]
        data_tools=data_tools.cli:cli
    ''',
)