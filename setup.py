from setuptools import setup, find_packages

setup(name='lab-grown-sheets',
      version='0.1',
      description='Various helpful tools',
      packages = find_packages(),
      install_requires=['networkx>=1.11', 'numpy>=1.15'],
      zip_safe=False)
