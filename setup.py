import setuptools


with open('version') as f:
    version = f.read().strip()
with open('requirements.txt') as f:
    required = f.read().splitlines()


setuptools.setup(
    name='dbi_repositories',
    version=version,
    author='Tim Niven',
    author_email='tim@doublethinklab.org',
    description='Reusable Repository pattern implementations for multiple DBs.',
    url=f'https://github.com/timniven/dbi-repositories.git#{version}',
    packages=setuptools.find_packages(),
    python_requires='>=3.8',
    install_requires=required)
