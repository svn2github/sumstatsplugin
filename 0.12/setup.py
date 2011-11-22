from setuptools import setup

PACKAGE = 'TracSumStats'
VERSION = '0.9.0'

setup(
    name=PACKAGE, version=VERSION,
    description='Sums a field for Roadmap/Milestone stats',
    author="Rob Guttman", author_email="guttman@alum.mit.edu",
    license='GPL', url='http://trac-hacks.org/wiki/TracSumStatsPlugin',
    packages = ['sumstats'],
    package_data = {'sumstats':['templates/*.css']},
    entry_points = {'trac.plugins':['sumstats.web_ui = sumstats.web_ui']}
)
