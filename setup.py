from setuptools import setup

setup(
    author="azcoigreach",
    author_email="azcoigreach@gmail.com",
    name = 'MongoDB Data Tools',
    version = '0.2.0',
    packages=['data_tools','data_tools.commands'],
    include_package_data=True,
    install_requires = [
        'click',
        'colorama',
        'coloredlogs',
        'pymongo',        
    ],
    entry_points = '''
        [console_scripts]
        data_tools=data_tools.cli:cli
    ''',
)