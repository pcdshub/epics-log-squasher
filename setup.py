import sys
from os import path

from setuptools import find_packages, setup

import versioneer

min_version = (3, 7)

if sys.version_info < min_version:
    error = """
epics-log-squasher does not support Python {0}.{1}.
Python {2}.{3} and above is required. Check your Python version like so:

python3 --version

This may be due to an out-of-date pip. Make sure you have pip >= 9.0.1.
Upgrade pip like so:

pip install --upgrade pip
""".format(*sys.version_info[:2], *min_version)
    sys.exit(error)


here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as readme_file:
    readme = readme_file.read()

with open(path.join(here, 'requirements.txt')) as requirements_file:
    # Parse requirements.txt, ignoring any commented-out lines.
    requirements = [line for line in requirements_file.read().splitlines()
                    if not line.startswith('#')]


git_requirements = [r for r in requirements if r.startswith('git+')]
if git_requirements:
    print('User must install the following packages manually:')
    print()
    print("\n".join(f'* {r}' for r in git_requirements))
    print()


setup(
    name='epics_log_squasher',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    license='BSD',
    author='SLAC National Accelerator Laboratory',
    packages=find_packages(exclude=['docs', 'tests']),
    description='EPICS IOC Output Squashing/Filtering Tool',
    long_description=readme,
    long_description_content_type="text/x-rst",
    url='https://github.com/pcdshub/epics-log-squasher',  # noqa
    entry_points={
        "console_scripts": [
            "epics-log-squasher=epics_log_squasher.bin.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        'epics_log_squasher': [
            # When adding files here, remember to update MANIFEST.in as well,
            # or else they will not be included in the distribution on PyPI!
            # 'path/to/data_file',
        ]
    },
    install_requires=requirements,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
    ],
)
