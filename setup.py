import os
from setuptools import setup, Command

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')

setup(
    name = "stormdb",
    version = "0.4b",
    author = "Christopher Bailey",
    author_email = "cjb@cfin.au.dk",
    description = ("Access to StormDb @ CFIN"),
    license = "BSD",
    keywords = "code",
    url = "https://github.com/cfin-tools/stormdb.git",
    packages=['stormdb'],
    scripts=['bin/submit_to_cluster'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
    cmdclass={
        'clean': CleanCommand,
    }

)
